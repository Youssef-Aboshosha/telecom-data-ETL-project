def show_gold_schema():
    docs = """
============================================================
📚 GOLD LAYER DATA WAREHOUSE SCHEMA DOCUMENTATION
============================================================

1. dim_customer (SCD Type 2)
Tracks who the customers are and their history.
| Column                | Description                        |
| :------------------- | :--------------------------------- |
| customer_sk (PK)      | Unique Surrogate Key (UUID)        |
| customer_id           | Original ID from Silver            |
| first_name/last_name  | Customer Name                      |
| email                 | Contact information                |
| city_id               | Foreign Key to Geography           |
| is_current            | Boolean (True = Latest record)     |
| start_date/end_date   | Validity period for the record     |

------------------------------------------------------------

2. dim_phone
Maps the physical asset (Phone) to its current status and plan.
| Column                | Description                                |
| :------------------- | :----------------------------------------- |
| phone_sk (PK)         | Unique Surrogate Key (UUID)                |
| phone_id              | Original ID from Silver                    |
| phone_number          | The actual MSISDN                          |
| sim_number            | Linked SIM card ID                         |
| plan_id               | Current assigned Service Plan              |
| plan_name             | Name of the plan (e.g., "Pro Max 5G")      |
| subscription_status   | Active, Suspended, or Terminated           |
| is_active             | Boolean flag for quick filtering           |

------------------------------------------------------------

3. dim_plan_catalog (Unified Catalog)
The master list of everything you sell (Base Plans & Add-ons).
| Column                | Description                        |
| :------------------- | :--------------------------------- |
| plan_sk (PK)          | Hashed Key (sha2)                  |
| source_id             | Original Plan or Add-on ID         |
| item_name             | Name of the product                |
| item_type             | BASE_PLAN or ADDON                 |
| price                 | Cost in Decimal format             |

------------------------------------------------------------

4. dim_geography
The spatial hierarchy for regional reporting.
| Column                | Description                        |
| :------------------- | :--------------------------------- |
| geography_sk (PK)     | Unique Surrogate Key (UUID)        |
| city_id               | Original City ID                   |
| city_name             | Name of the City                   |
| country_id            | Original Country ID                |
| country_name          | Name of the Country                |
| country_code          | ISO Code (e.g., "EG", "US")        |

------------------------------------------------------------

5. dim_date
The time-intelligence engine.
| Column                | Description                        |
| :------------------- | :--------------------------------- |
| date_key (PK)         | Integer (e.g., 20260409)           |
| full_date             | Date format                        |
| day / month / year    | Temporal components                |
| day_of_week           | Monday, Tuesday, etc.              |
| is_weekend            | Boolean flag                       |

============================================================
    """
    print(docs)

if __name__ == "__main__":
    show_gold_schema()
