import pandas as pd
import sqlite3
import os


def run_etl():
    print("STARTED ETL PROCESS...")

    # --- PHASE 1: EXTRACT ---
    print("[1] Extracting data from heterogeneous sources...")

    # Source 1: CSV
    try:
        df_customers = pd.read_csv("customers.csv")
        print(f"   -> Loaded {len(df_customers)} customers from CSV.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Source 2: SQLite
    try:
        conn = sqlite3.connect("orders.db")
        df_orders = pd.read_sql_query("SELECT * FROM orders", conn)
        print(f"   -> Loaded {len(df_orders)} orders from SQLite.")
    except Exception as e:
        print(f"Error reading DB: {e}")
        return
    finally:
        if conn:
            conn.close()

    #---------------------------------------------------------------------------------
    # # --- PHASE 2: TRANSFORM ---
    # print("[2] Transforming data...")

    # # Tổng tiền theo customer_id
    # orders_sum = (
    #     df_orders
    #     .groupby("customer_id")["amount"]
    #     .sum()
    #     .reset_index()
    # )

    # # LEFT JOIN từ customers (GIỮ ĐỦ 4 NGƯỜI)
    # report_df = pd.merge(
    #     df_customers,
    #     orders_sum,
    #     left_on="id",
    #     right_on="customer_id",
    #     how="left"
    # )

    # # Khách không có order → 0
    # report_df["amount"] = report_df["amount"].fillna(0)

    # # Chọn cột + đổi tên
    # report_df = report_df[["name", "email", "amount"]]
    # report_df.columns = ["Customer Name", "Email", "Total Spent"]

    #---------------------------------------------------------------------------------
    # --- PHASE 2: TRANSFORM ---
    print("[2] Transforming data...")

    # Join data (Data Stitching)
    merged_df = pd.merge(
        df_orders,
        df_customers,
        left_on="customer_id",
        right_on="id",
        how="left"
    )

    # Aggregate total spent per customer
    report_df = (
        merged_df
        .groupby(["name", "email"])["amount"]
        .sum()
        .reset_index()
    )

    # Rename columns
    report_df.columns = ["Customer Name", "Email", "Total Spent"]

    # 1. Chỉ giữ khách có tổng chi tiêu > 500
    report_df = report_df[report_df["Total Spent"] > 500]

    # 2. Sắp xếp theo tổng chi tiêu giảm dần
    report_df = report_df.sort_values(
        by="Total Spent",
        ascending=False
    )

    # --- PHASE 3: LOAD ---
    print("[3] Loading Data to Report...")
    print("\n--- FINAL REPORT ---")
    print(report_df)

    report_df.to_csv("final_report.csv", index=False)
    print("\nReport saved to 'final_report.csv'")


if __name__ == "__main__":
    # Create DB if not exists (Docker first run)
    if not os.path.exists("orders.db"):
        import init_db

    run_etl()