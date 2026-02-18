import json
import re
from dataclasses import dataclass, asdict
from typing import List

@dataclass
class ColumnSchema:
    name: str
    data_type: str
    description: str = ""

@dataclass
class TableSchema:
    table_name: str
    physical_name: str   
    table_type: str       
    domain: str
    columns: List[ColumnSchema]
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

raw_metadata = [
    {
        "table_name": "foundation.financial.ledger_lines_enriched_v1",
        "columns": [
            {"name": "ledger_line_id", "type": "bigint"},
            {"name": "account_id", "type": "varchar"},
            {"name": "transaction_id", "type": "varchar"},
            {"name": "entry_date", "type": "timestamp"},
            {"name": "amount", "type": "decimal"}
        ]
    },
    {
        "table_name": "foundation.financial.deposits_v4",
        "columns": [
            {"name": "deposit_id", "type": "bigint"},
            {"name": "user_id", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
            {"name": "method", "type": "varchar"},
            {"name": "deposit_date", "type": "timestamp"}
        ]
    },
    {
        "table_name": "foundation.financial.deposits_enriched_v1",
        "columns": [
            {"name": "deposit_id", "type": "bigint"},
            {"name": "user_id", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
            {"name": "transaction_id", "type": "varchar"},
            {"name": "status", "type": "varchar"}
        ]
    },
    {
        "table_name": "foundation.financial.withdrawals_v4",
        "columns": [
            {"name": "withdrawal_id", "type": "bigint"},
            {"name": "user_id", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
            {"name": "withdrawal_date", "type": "timestamp"},
            {"name": "method", "type": "varchar"}
        ]
    },
    {
        "table_name": "foundation.financial.withdrawals_enriched_v4",
        "columns": [
            {"name": "withdrawal_id", "type": "bigint"},
            {"name": "user_id", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
            {"name": "status", "type": "varchar"},
            {"name": "transaction_id", "type": "varchar"}
        ]
    },
    {
        "table_name": "foundation.financial.ledger_account_balances_v1",
        "columns": [
            {"name": "account_id", "type": "varchar"},
            {"name": "user_id", "type": "varchar"},
            {"name": "balance", "type": "decimal"},
            {"name": "as_of_date", "type": "date"},
            {"name": "currency", "type": "varchar"}
        ]
    },
    {
        "table_name": "foundation.financial.worldpay_transactions",
        "columns": [
            {"name": "transaction_id", "type": "varchar"},
            {"name": "transaction_type", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
            {"name": "status", "type": "varchar"},
            {"name": "user_id", "type": "varchar"}
        ]
    },
    {
        "table_name": "foundation.account.authgateway_session_created_events",
        "columns": [
            {"name": "session_id", "type": "varchar"},
            {"name": "user_id", "type": "varchar"},
            {"name": "success", "type": "varchar"},
            {"name": "device_type", "type": "varchar"},
            {"name": "created_at", "type": "timestamp"}
        ]
    },
    {
        "table_name": "foundation.account.verification_attempt_v1",
        "columns": [
            {"name": "attempt_id", "type": "varchar"},
            {"name": "user_id", "type": "varchar"},
            {"name": "verification_type", "type": "varchar"},
            {"name": "result", "type": "varchar"},
            {"name": "attempted_at", "type": "timestamp"}
        ]
    },
    {
        "table_name": "foundation.account.verified_user_details",
        "columns": [
            {"name": "user_id", "type": "varchar"},
            {"name": "verification_level", "type": "varchar"},
            {"name": "verified_date", "type": "timestamp"},
            {"name": "country", "type": "varchar"},
            {"name": "active", "type": "varchar"}
        ]
    }
]

# ============================
# STEP 2: NORMALIZATION FUNCTIONS
# ============================

def get_table_type(table_name):
    """Distinguish between Raw and Enriched physical tables."""
    if "_enriched" in table_name.lower():
        return "ENRICHED"
    return "RAW"

def normalize_table_name(table_name):
    """Strips suffixes to group by business concept."""
    name = table_name.lower()
    name = re.sub(r'_v\d+', '', name)
    name = re.sub(r'_enriched', '', name)
    name = name.replace('.', '_')
    return name

def normalize_column_name(column_name):
    name = column_name.strip()
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    name = s2.lower()
    name = re.sub(r'__+', '_', name)
    return name

def normalize_data_type(data_type):
    mapping = {
        "bigint": "integer",
        "int": "integer",
        "varchar": "string",
        "decimal": "decimal",
        "timestamp": "datetime",
        "date": "date"
    }
    return mapping.get(data_type.lower(), "string")

def assign_domain(table_name):
    if "financial" in table_name.lower(): return "Finance"
    if "account" in table_name.lower(): return "Account"
    return "General"

# ============================
# STEP 3: NORMALIZE & STRUCTURE
# ============================

table_objects = []

for table in raw_metadata:
    # Handle the Enriched vs Raw distinction here
    phys_name = table["table_name"]
    norm_name = normalize_table_name(phys_name)
    t_type = get_table_type(phys_name)
    
    columns = [
        ColumnSchema(
            name=normalize_column_name(col["name"]),
            data_type=normalize_data_type(col["type"])
        )
        for col in table["columns"]
    ]
    
    table_obj = TableSchema(
        table_name=norm_name,
        physical_name=phys_name,
        table_type=t_type,
        domain=assign_domain(phys_name),
        columns=columns,
        description=f"This is the {t_type} version of the {norm_name} concept."
    )
    table_objects.append(table_obj)

# ============================
# STEP 5: DEFINE RELATIONSHIPS
# ============================

relationships = [
    RelationshipSchema(
        source_table="foundation_financial_deposits",
        source_column="account_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="belongs_to",
        description="Deposits are made into accounts owned by verified users."
    ),
    RelationshipSchema(
        source_table="foundation_financial_withdrawals",
        source_column="account_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="belongs_to",
        description="Withdrawals are performed by users associated with accounts."
    ),
    RelationshipSchema(
        source_table="foundation_financial_ledger_account_balances",
        source_column="account_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="account_owner",
        description="Ledger account balances correspond to verified users."
    ),
    RelationshipSchema(
        source_table="foundation_account_authgateway_session_created_events",
        source_column="user_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="session_of",
        description="Authentication sessions belong to verified users."
    ),
    RelationshipSchema(
        source_table="foundation_account_verification_attempt",
        source_column="user_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="verification_event",
        description="Verification attempts relate to user identity validation."
    ),
    RelationshipSchema(
        source_table="foundation_financial_worldpay_transactions",
        source_column="account_id",
        target_table="foundation_account_verified_user_details",
        target_column="user_id",
        relationship_type="payment_transaction",
        description="Worldpay transactions originate from user-linked accounts."
    )
]

# ============================
# STEP 6: BUILD METADATA MODEL
# ============================

metadata_model = MetadataSchema(
    tables=table_objects,
    relationships=relationships
)

# ============================
# STEP 7: EXPORT TO JSON
# ============================

with open("ontology_ready_metadata.json", "w") as f:
    json.dump(asdict(metadata_model), f, indent=4)

print("✅ Ontology-ready metadata JSON created as 'ontology_ready_metadata.json'")