"""
Builds sample_data/sales_transactions.json from the uploaded
Master_Sales_data.xlsx, matching the Kafka message schema defined in the
PRD (6.1 Sales Transaction Schema):

    transaction_id, timestamp, store_id, product_id, quantity, price,
    unit_cost, cost_sales, sales_value

Source workbook layout (per sheet: Nov_file, Dec_file, Jan_file):
    Category, Stock Code, Description, Level, Sold Period,
    Unit Cost, Unit Price, Cost Sales, Sales Value, Profit, Profit %

Data mapping:
  - product_id     <- Stock Code
  - quantity       <- Sold Period
  - store_id       <- BELFAST
  - unit_cost      <- Unit Cost
  - cost_sales     <- Unit Cost * Sold Period
  - price          <- Unit Price
  - sales_value    <- Sales Value
  - timestamp      <- synthesized within the worksheet's month
  - transaction_id <- sequential, prefixed by worksheet/month
"""

import calendar
import json
import os

import openpyxl


SHEET_TO_MONTH = {
    "Nov_file": (2025, 11),
    "Dec_file": (2025, 12),
    "Jan_file": (2026, 1),
}

SOURCE_XLSX = (
    "D:/STUDY/Data_Science_Courses/PROJECTS/12.RetailBatch - Sales Data Aggregation Pipeline/RetailBatch---Sales-Data-Aggregation-Pipeline/data/Master_Sales_data.xlsx"
)

OUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sample_data",
    "sales_transactions.json",
)


def build_transactions():
    wb = openpyxl.load_workbook(SOURCE_XLSX, data_only=True)

    transactions = []
    tx_counter = 0

    for sheet_name, (year, month) in SHEET_TO_MONTH.items():
        ws = wb[sheet_name]

        # Get the actual number of days in the current month.
        days_in_month = calendar.monthrange(year, month)[1]

        # Reset the monthly counter for every worksheet/month.
        month_tx_counter = 0

        headers = [
            cell.value
            for cell in next(ws.iter_rows(min_row=1, max_row=1))
        ]

        col = {
            header: index
            for index, header in enumerate(headers)
        }

        for row in ws.iter_rows(min_row=2, values_only=True):
            stock_code = row[col["Stock Code"]]
            quantity = row[col["Sold Period"]]
            unit_cost = row[col["Unit Cost"]]
            unit_price = row[col["Unit Price"]]
            sales_value = row[col["Sales Value"]]

            # Skip incomplete rows.
            if (
                stock_code is None
                or quantity is None
                or unit_cost is None
                or unit_price is None
            ):
                continue

            quantity = int(quantity)
            unit_cost = round(float(unit_cost), 2)
            unit_price = round(float(unit_price), 2)

            # Cost Sales = Unit Cost * Sold Period
            cost_sales = round(unit_cost * quantity, 2)

            if sales_value is not None:
                sales_value = round(float(sales_value), 2)

            # Spread valid transactions evenly across every day
            # in the corresponding month.
            month_tx_counter += 1
            day = ((month_tx_counter - 1) % days_in_month) + 1

            timestamp = (
                f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z"
            )

            tx_counter += 1

            transactions.append(
                {
                    "transaction_id": (
                        f"TXN-{sheet_name[:3].upper()}-"
                        f"{tx_counter:06d}"
                    ),
                    "timestamp": timestamp,
                    "store_id": "BELFAST",
                    "product_id": str(stock_code),
                    "quantity": quantity,
                    "price": unit_price,
                    "unit_cost": unit_cost,
                    "cost_sales": cost_sales,
                    "sales_value": sales_value,
                }
            )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(OUT_PATH, "w", encoding="utf-8") as output_file:
        json.dump(transactions, output_file, indent=2)

    print(
        f"Wrote {len(transactions)} transactions to {OUT_PATH}"
    )

    return transactions


if __name__ == "__main__":
    build_transactions()