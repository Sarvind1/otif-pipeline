"""
Output Folder Management Utility
=================================

Utility script for managing the organized output folder structure of the TAT Calculator.
Provides functions to create, clean, and inspect output folders.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

class OutputFolderManager:
    """Manages the organized output folder structure for TAT Calculator"""
    
    FOLDER_STRUCTURE = {
        'outputs/tat_results': 'TAT calculation JSON results',
        'outputs/delay_results': 'Delay analysis JSON results',
        'outputs/excel_exports': 'Excel files (exports, reports)',
        'outputs/csv_files': 'Processed CSV data files',
        'outputs/logs': 'Application logs and error reports'
    }
    
    def __init__(self):
        self.base_path = Path('.')
        
    def create_folders(self):
        """Create all required output folders"""
        print("Creating organized output folder structure...")
        
        for folder, description in self.FOLDER_STRUCTURE.items():
            folder_path = self.base_path / folder
            folder_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created: {folder} - {description}")
        
        print("\n📁 Folder structure created successfully!")
        self.show_structure()
    
    def show_structure(self):
        """Display the current folder structure"""
        print("\n📂 Current Output Structure:")
        print("├── outputs/")
        
        for folder in self.FOLDER_STRUCTURE.keys():
            if folder == 'outputs/logs':  # Last item
                print(f"    └── {folder.split('/')[-1]}/")
            else:
                print(f"    ├── {folder.split('/')[-1]}/")
        
        print()
        self._show_file_counts()
    
    def _show_file_counts(self):
        """Show file counts in each folder"""
        print("📊 File counts:")
        for folder in self.FOLDER_STRUCTURE.keys():
            folder_path = self.base_path / folder
            if folder_path.exists():
                file_count = len([f for f in folder_path.iterdir() if f.is_file()])
                print(f"    {folder.split('/')[-1]}: {file_count} files")
            else:
                print(f"    {folder.split('/')[-1]}: folder not found")
    
    def clean_old_files(self, days_old: int = 30):
        """Clean files older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cleaned_files = []
        
        print(f"🧹 Cleaning files older than {days_old} days...")
        
        for folder in self.FOLDER_STRUCTURE.keys():
            folder_path = self.base_path / folder
            if not folder_path.exists():
                continue
                
            for file_path in folder_path.iterdir():
                if file_path.is_file():
                    file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_modified < cutoff_date:
                        try:
                            file_path.unlink()
                            cleaned_files.append(str(file_path))
                            print(f"    Deleted: {file_path.name}")
                        except Exception as e:
                            print(f"    Error deleting {file_path.name}: {e}")
        
        if cleaned_files:
            print(f"\n✅ Cleaned {len(cleaned_files)} old files")
        else:
            print(f"\n💡 No files older than {days_old} days found")
        
        return cleaned_files
    
    def archive_results(self, archive_name: str = None):
        """Archive all results to a timestamped folder"""
        if archive_name is None:
            archive_name = f"archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        archive_path = self.base_path / 'archived_outputs' / archive_name
        archive_path.mkdir(parents=True, exist_ok=True)
        
        archived_count = 0
        
        print(f"📦 Archiving results to: {archive_path}")
        
        for folder in self.FOLDER_STRUCTURE.keys():
            source_folder = self.base_path / folder
            if source_folder.exists() and any(source_folder.iterdir()):
                dest_folder = archive_path / folder.split('/')[-1]
                shutil.copytree(source_folder, dest_folder, dirs_exist_ok=True)
                
                file_count = len([f for f in dest_folder.rglob('*') if f.is_file()])
                archived_count += file_count
                print(f"    Archived {file_count} files from {folder}")
        
        print(f"\n✅ Archive complete: {archived_count} files archived to {archive_name}")
        return str(archive_path)
    
    def generate_report(self):
        """Generate a summary report of the output structure"""
        report = {
            "generated_at": datetime.now().isoformat(),
            "folder_structure": {},
            "summary": {
                "total_folders": len(self.FOLDER_STRUCTURE),
                "total_files": 0,
                "oldest_file": None,
                "newest_file": None
            }
        }
        
        oldest_time = None
        newest_time = None
        
        for folder, description in self.FOLDER_STRUCTURE.items():
            folder_path = self.base_path / folder
            folder_info = {
                "description": description,
                "exists": folder_path.exists(),
                "file_count": 0,
                "files": []
            }
            
            if folder_path.exists():
                for file_path in folder_path.iterdir():
                    if file_path.is_file():
                        file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                        file_info = {
                            "name": file_path.name,
                            "size_kb": round(file_path.stat().st_size / 1024, 2),
                            "modified": file_modified.isoformat()
                        }
                        folder_info["files"].append(file_info)
                        folder_info["file_count"] += 1
                        
                        # Track oldest and newest
                        if oldest_time is None or file_modified < oldest_time:
                            oldest_time = file_modified
                            report["summary"]["oldest_file"] = {
                                "path": str(file_path),
                                "modified": file_modified.isoformat()
                            }
                        
                        if newest_time is None or file_modified > newest_time:
                            newest_time = file_modified
                            report["summary"]["newest_file"] = {
                                "path": str(file_path),
                                "modified": file_modified.isoformat()
                            }
            
            report["folder_structure"][folder] = folder_info
            report["summary"]["total_files"] += folder_info["file_count"]
        
        return report
    
    def save_report(self, filename: str = None):
        """Save the structure report to a JSON file"""
        if filename is None:
            filename = f"outputs/logs/structure_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Ensure logs folder exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        report = self.generate_report()
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📋 Structure report saved to: {filename}")
        return filename


def main():
    """Main function for interactive folder management"""
    manager = OutputFolderManager()
    
    print("TAT Calculator - Output Folder Manager")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("1. Create folder structure")
        print("2. Show current structure")
        print("3. Clean old files")
        print("4. Archive current results")
        print("5. Generate structure report")
        print("6. Exit")
        
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == '1':
            manager.create_folders()
        
        elif choice == '2':
            manager.show_structure()
        
        elif choice == '3':
            days = input("Enter days old (default 30): ").strip()
            days = int(days) if days.isdigit() else 30
            manager.clean_old_files(days)
        
        elif choice == '4':
            archive_name = input("Archive name (press Enter for auto): ").strip()
            if not archive_name:
                archive_name = None
            manager.archive_results(archive_name)
        
        elif choice == '5':
            filename = manager.save_report()
            print(f"Report generated with {manager.generate_report()['summary']['total_files']} total files")
        
        elif choice == '6':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please select 1-6.")


if __name__ == "__main__":
    main()
