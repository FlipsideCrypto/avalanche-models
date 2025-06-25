#!/usr/bin/env python3
"""
Chain Name Standardizer

This utility standardizes chain names across multiple CSV files using a centralized mapping table.
It handles case-insensitive matching and can process multiple files at once.
"""

import pandas as pd
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

class ChainNameStandardizer:
    def __init__(self, mapping_file: str = "data/chain_name_standardization_mapping.csv"):
        """Initialize the standardizer with a mapping file."""
        self.mapping_file = mapping_file
        self.mapping_dict = self._load_mapping()
    
    def _load_mapping(self) -> Dict[str, str]:
        """Load the chain name mapping from CSV file."""
        try:
            df = pd.read_csv(self.mapping_file)
            # Create a dictionary with lowercase keys for case-insensitive matching
            mapping = {}
            for _, row in df.iterrows():
                source_name = row['source_name'].strip().lower()
                standardized_name = row['standardized_name'].strip()
                mapping[source_name] = standardized_name
            return mapping
        except FileNotFoundError:
            print(f"Error: Mapping file {self.mapping_file} not found!")
            return {}
        except Exception as e:
            print(f"Error loading mapping file: {e}")
            return {}
    
    def standardize_name(self, chain_name: str) -> str:
        """Standardize a single chain name."""
        if not chain_name:
            return chain_name
        
        # Try exact match first (case-insensitive)
        normalized_name = chain_name.strip().lower()
        if normalized_name in self.mapping_dict:
            return self.mapping_dict[normalized_name]
        
        # Try with common suffixes removed
        suffixes_to_remove = [' mainnet', ' chain', ' network', ' smart chain']
        for suffix in suffixes_to_remove:
            if normalized_name.endswith(suffix):
                base_name = normalized_name[:-len(suffix)]
                if base_name in self.mapping_dict:
                    return self.mapping_dict[base_name]
        
        # If no match found, return original (or you could log it for review)
        print(f"Warning: No mapping found for '{chain_name}'")
        return chain_name
    
    def standardize_csv_file(self, file_path: str, chain_column: str, output_file: Optional[str] = None) -> bool:
        """Standardize chain names in a CSV file."""
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            if chain_column not in df.columns:
                print(f"Error: Column '{chain_column}' not found in {file_path}")
                return False
            
            # Create a copy of the original column
            original_column = f"{chain_column}_original"
            df[original_column] = df[chain_column]
            
            # Standardize the chain names
            df[chain_column] = df[chain_column].apply(self.standardize_name)
            
            # Determine output file
            if output_file is None:
                output_file = file_path
            
            # Save the standardized file
            df.to_csv(output_file, index=False)
            
            # Print summary
            changed_count = (df[original_column] != df[chain_column]).sum()
            print(f"Processed {file_path}: {changed_count} chain names standardized")
            
            return True
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return False
    
    def standardize_multiple_files(self, file_patterns: List[str], chain_column: str) -> None:
        """Standardize chain names in multiple files."""
        for pattern in file_patterns:
            files = list(Path("data").glob(pattern))
            for file_path in files:
                self.standardize_csv_file(str(file_path), chain_column)
    
    def find_unmapped_chains(self, file_patterns: List[str], chain_column: str) -> set:
        """Find chain names that don't have mappings."""
        unmapped = set()
        
        for pattern in file_patterns:
            files = list(Path("data").glob(pattern))
            for file_path in files:
                try:
                    df = pd.read_csv(file_path)
                    if chain_column in df.columns:
                        for chain_name in df[chain_column].dropna().unique():
                            normalized = chain_name.strip().lower()
                            if normalized not in self.mapping_dict:
                                unmapped.add(chain_name)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
        
        return unmapped

def main():
    """Main function for command-line usage."""
    if len(sys.argv) < 2:
        print("Usage: python chain_name_standardizer.py <command> [options]")
        print("Commands:")
        print("  standardize <file> <column> [output_file] - Standardize a single file")
        print("  batch <pattern> <column> - Standardize multiple files")
        print("  find-unmapped <pattern> <column> - Find unmapped chain names")
        return
    
    standardizer = ChainNameStandardizer()
    
    command = sys.argv[1]
    
    if command == "standardize":
        if len(sys.argv) < 4:
            print("Usage: python chain_name_standardizer.py standardize <file> <column> [output_file]")
            return
        
        file_path = sys.argv[2]
        chain_column = sys.argv[3]
        output_file = sys.argv[4] if len(sys.argv) > 4 else None
        
        success = standardizer.standardize_csv_file(file_path, chain_column, output_file)
        if success:
            print(f"Successfully standardized {file_path}")
    
    elif command == "batch":
        if len(sys.argv) < 4:
            print("Usage: python chain_name_standardizer.py batch <pattern> <column>")
            return
        
        pattern = sys.argv[2]
        chain_column = sys.argv[3]
        
        standardizer.standardize_multiple_files([pattern], chain_column)
    
    elif command == "find-unmapped":
        if len(sys.argv) < 4:
            print("Usage: python chain_name_standardizer.py find-unmapped <pattern> <column>")
            return
        
        pattern = sys.argv[2]
        chain_column = sys.argv[3]
        
        unmapped = standardizer.find_unmapped_chains([pattern], chain_column)
        if unmapped:
            print("Unmapped chain names found:")
            for chain in sorted(unmapped):
                print(f"  - {chain}")
        else:
            print("All chain names are mapped!")

if __name__ == "__main__":
    main() 