import json
import re
import pandas as pd
import glob
from dataclasses import dataclass, asdict
from typing import List, Dict

# ============================
# STEP 1: SCHEMAS
# ============================

@dataclass
class ColumnSchema:
    name: str
    data_type: str
    canonical_id: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    description: str = ""

@dataclass
class TableSchema:
    table_name: str
    physical_name: str   
    table_type: str       
    domain: str
    columns: List[ColumnSchema]
    canonical_id: str
    description: str = ""

@dataclass
class RelationshipSchema:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str
    description: str = ""

@dataclass
class MetadataSchema:
    tables: List[TableSchema]
    relationships: List[RelationshipSchema]

# ============================
# STEP 2: REFINED NORMALIZATION
# ============================

TARGET_TABLES = [
    "deposits", "deposits_enriched", "ledger_account_balances_v1", 
    "ledger_lines_enriched", "verification_attempt", 
    "verified_user_details", "withdrawals", "worldpay_transactions"
]

def normalize_table_name(phys_name):
    # Strip quotes and schema prefixes like "foundation_views.financial".
    name = phys_name.lower().replace('"', '')
    if "." in name:
        name = name.split(".")[-1]
    
    # Check if it matches your specific 8 tables
    for target in TARGET_TABLES:
        if target in name:
            # Standardize suffixes for canonical naming
            clean = target.replace('_v1', '').replace('_enriched', '')
            return clean
    return name.replace('.', '_')

def normalize_column_name(column_name):
    return column_name.strip().lower()

def normalize_data_type(data_type):
    mapping = {"bigint": "integer", "int": "integer", "varchar": "string", "decimal": "decimal"}
    return mapping.get(str(data_type).lower(), "string")

# ============================
# STEP 3: AUTOMATED RELATIONSHIP DISCOVERY
# ============================

def discover_relationships(tables: List[TableSchema]) -> List[RelationshipSchema]:
    relationships = []
    # Identify potential 'Master' tables (usually 'details' or 'user' tables)
    master_table_ids = [t.canonical_id for t in tables if "user" in t.canonical_id or "details" in t.canonical_id]
    
    for source_table in tables:
        for col in source_table.columns:
            # Heuristic: If a column name is found in a master table, create a relationship
            # e.g., 'user_id' in 'deposits' links to 'user_id' in 'verified_user_details'
            for target_table in tables:
                if source_table.canonical_id == target_table.canonical_id:
                    continue
                
                for target_col in target_table.columns:
                    if col.name == target_col.name and ("id" in col.name):
                        # Flag as FK in the source
                        col.is_foreign_key = True
                        
                        relationships.append(RelationshipSchema(
                            source_table=source_table.canonical_id,
                            source_column=col.canonical_id,
                            target_table=target_table.canonical_id,
                            target_column=target_col.canonical_id,
                            relationship_type="linked_by_id",
                            description=f"Auto-detected link via {col.name}"
                        ))
    return relationships

# ============================
# STEP 4: PROCESSING ENGINE
# ============================

def process_csv_files(path_pattern: str):
    all_tables = {}
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file)
        for _, row in df.iterrows():
            key = str(row.get('key', ''))
            if not key or "." not in key: continue
            
            # Logic: Parse key to find which of the 8 tables this belongs to
            phys_table = ".".join(key.split('.')[:-1])
            norm_table = normalize_table_name(phys_table)
            
            if norm_table not in all_tables:
                all_tables[norm_table] = {"phys": phys_table, "cols": [], "desc": ""}
            
            # If row defines the table properties
            if "otype=table" in str(row.get('al_datadict_item_properties', '')):
                all_tables[norm_table]["desc"] = row.get('description', '')
            else:
                # Row defines a column
                col_raw = key.split('.')[-1]
                col_name = normalize_column_name(col_raw)
                
                all_tables[norm_table]["cols"].append(ColumnSchema(
                    name=col_name,
                    data_type=normalize_data_type(row.get('al_datadict_item_column_data_type')),
                    canonical_id=f"{norm_table}.{col_name}",
                    is_primary_key=("id" in col_name and norm_table in col_name),
                    description=str(row.get('description', '')) if pd.notna(row.get('description')) else ""
                ))

    # Convert to objects
    table_objects = []
    for name, data in all_tables.items():
        table_objects.append(TableSchema(
            table_name=name,
            physical_name=data["phys"],
            table_type="ENRICHED" if "enriched" in data["phys"] else "RAW",
            domain="Finance" if "financial" in data["phys"] else "Account",
            columns=data["cols"],
            canonical_id=name,
            description=data["desc"]
        ))
    
    # Auto-discover relationships based on shared IDs
    rels = discover_relationships(table_objects)
    
    return MetadataSchema(tables=table_objects, relationships=rels)

# ============================
# STEP 5: RUN
# ============================

# Point this to your 8 CSV files
final_metadata = process_csv_files("C:/Users/denis/OneDrive/Documents/Fanduel-project-1/data_dictionaries/*.csv")

with open("ontology_ready_metadata.json", "w") as f:
    json.dump(asdict(final_metadata), f, indent=4)

print(f"Generated {len(final_metadata.tables)} tables and {len(final_metadata.relationships)} auto-detected relationships.")