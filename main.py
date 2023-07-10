from rich import print
from pydantic import BaseModel
from typing import List, Dict


class ETL_OM_FOCUS_DCF(BaseModel):
    item: str = None
    description: str = None
    uom: str = None
    unit_cost: float = None
    qty_sold: int = None
    extended_cost: float = None
    fee_percentage: float = None
    admin_fee: float = None
    calculated_admin_fee: float = None
    match: bool = False


class ETL_ADJ(BaseModel):
    total_cost: float = None
    total_fee: float = None
    fee_percentage: float = None
    adj: float = None
    total_fee_after_adj: float = None


def etl(file_name):
    import os
    import re
    from pathlib import Path

    """
    Extract, Transform, Load
    """
    # Extract
    print("Extracting...")
    base_path = r"C:\Temp"
    file_path = Path(os.path.join(base_path, file_name))
    with file_path.open("r") as f:
        data = f.readlines()

    # Transform
    print("Transforming...")
    adj = [
        x
        for x in data
        if re.search(r"(program total:|debit adjustment:|program total after)", x, re.I)
    ]
    data = [x for x in data if x.startswith("                 ") and x[25] != " "]

    # Load
    print("Loading...")
    total_start = 91
    total_end = total_start + 20
    total_fee_start = total_end + 1
    adj_start = total_fee_start
    total_fee_after_adj_start = total_fee_start

    adjustments: Dict[str, ETL_ADJ] = {
        "3.2": None,
        "3.5": None,
    }

    temp = [{}, {}]
    count = 0
    for row in adj:
        if count > 1:
            continue

        if re.search(r"PROGRAM TOTAL:", row, re.I):
            temp[count]["total_cost"] = (
                float(
                    "-"
                    + row[total_start:total_end]
                    .strip()
                    .replace(",", "")
                    .replace("-", "")
                )
                if row[total_start:total_end].endswith("-")
                else float(row[total_start:total_end].strip().replace(",", ""))
            )
            temp[count]["total_fee"] = (
                float(
                    "-"
                    + row[total_fee_start:].strip().replace(",", "").replace("-", "")
                )
                if row[total_fee_start:].strip().endswith("-")
                else float(
                    row[total_fee_start:].strip().replace(",", "").replace("-", "")
                )
            )

            temp[count]["fee_percentage"] = round(
                temp[count]["total_fee"] / temp[count]["total_cost"] * 100, 2
            )

        elif re.search(r"DEBIT ADJUSTMENT:", row, re.I):
            temp[count]["adj"] = (
                float("-" + row[adj_start:].strip().replace(",", "").replace("-", ""))
                if row[adj_start:].strip().endswith("-")
                else float(row[adj_start:].strip().replace(",", "").replace("-", ""))
            )

        elif re.search(r"PROGRAM TOTAL AFTER ADJUSTMENT:", row, re.I):
            temp[count]["total_fee_after_adj"] = (
                float(
                    "-"
                    + row[total_fee_after_adj_start:]
                    .strip()
                    .replace(",", "")
                    .replace("-", "")
                )
                if row[total_fee_after_adj_start:].strip().endswith("-")
                else float(
                    row[total_fee_after_adj_start:]
                    .strip()
                    .replace(",", "")
                    .replace("-", "")
                )
            )

            adjustments[str(temp[count]["fee_percentage"])] = ETL_ADJ(**temp[count])

            count += 1

    item_start = 29
    item_end = 35
    description_start = item_end + 4
    description_end = description_start + 28
    uom_start = description_end + 2
    uom_end = uom_start + 2
    unit_cost_start = uom_end + 1
    unit_cost_end = unit_cost_start + 12
    qty_sold_start = unit_cost_end + 1
    qty_sold_end = qty_sold_start + 11
    extended_cost_start = qty_sold_end + 1
    extended_cost_end = extended_cost_start + 14
    fee_percentage_start = extended_cost_end + 1
    fee_percentage_end = fee_percentage_start + 10
    admin_fee_start = fee_percentage_end + 1
    admin_fee_end = -1

    rows: List[ETL_OM_FOCUS_DCF] = []
    error_rows: List[ETL_OM_FOCUS_DCF] = []

    for row in data:
        temp = {
            "item": row[item_start:item_end].strip().lstrip("0"),
            "description": row[description_start:description_end].strip(),
            "uom": row[uom_start:uom_end].strip(),
            "unit_cost": float(
                row[unit_cost_start:unit_cost_end].strip().replace(",", "")
            ),
            "qty_sold": int(
                "-"
                + row[qty_sold_start:qty_sold_end]
                .strip()
                .replace(",", "")
                .replace("-", "")
            )
            if row[qty_sold_start:qty_sold_end].strip().endswith("-")
            else int(row[qty_sold_start:qty_sold_end].strip().replace(",", "")),
            "extended_cost": float(
                "-"
                + row[extended_cost_start:extended_cost_end]
                .strip()
                .replace(",", "")
                .replace("-", "")
            )
            if row[extended_cost_start:extended_cost_end].strip().endswith("-")
            else float(
                row[extended_cost_start:extended_cost_end].strip().replace(",", "")
            ),
            "fee_percentage": float(
                row[fee_percentage_start:fee_percentage_end].strip()
            ),
            "admin_fee": float(
                "-"
                + row[admin_fee_start:admin_fee_end]
                .strip()
                .replace(",", "")
                .replace("-", "")
            )
            if row[admin_fee_start:admin_fee_end].strip().endswith("-")
            else float(row[admin_fee_start:admin_fee_end].strip().replace(",", "")),
        }

        temp["calculated_admin_fee"] = round(
            temp["extended_cost"] * temp["fee_percentage"] / 100, 2
        )
        temp["match"] = temp["calculated_admin_fee"] == temp["admin_fee"] or (
            abs(abs(temp["calculated_admin_fee"]) - abs(temp["admin_fee"])) < 0.05
        )

        if temp["match"]:
            temp["calculated_admin_fee"] = temp["admin_fee"]

        temp = ETL_OM_FOCUS_DCF(**temp)

        if temp.match:
            rows.append(temp)
        else:
            error_rows.append(temp)

    # Return
    print("Done!")
    import pandas as pd
    from datetime import datetime

    return_cols = [
        "item",
        "description",
        "uom",
        "unit_cost",
        "qty_sold",
        "extended_cost",
        "fee_percentage",
        "admin_fee",
    ]

    print(adjustments)

    with pd.ExcelWriter(
        os.path.join(base_path, f"{datetime.now():%Y%m%d%H%S}.{file_name}.xlsx"),
    ) as writer:

        df1 = pd.DataFrame([x.dict() for x in rows if x.fee_percentage > 3.2])
        df1["match"] = df1["match"].astype(str)
        df1.loc["total"] = df1.sum(numeric_only=True)
        df1.loc[df1.index[-1], "fee_percentage"] = 3.5
        df1.loc["adjustment", "fee_percentage"] = "Adjustment"
        df1.loc["adjustment", "admin_fee"] = adjustments["3.5"].adj
        df1.loc["total_after_adj", "fee_percentage"] = "Total After Adjustment"
        df1.loc["total_after_adj", "admin_fee"] = adjustments["3.5"].total_fee_after_adj
        df1[return_cols].to_excel(writer, sheet_name="3.5%", index=False)

        df2 = pd.DataFrame([x.dict() for x in rows if x.fee_percentage <= 3.2])
        df2["match"] = df2["match"].astype(str)
        df2.loc["total"] = df2.sum(numeric_only=True)
        df2.loc[df2.index[-1], "fee_percentage"] = 3.2
        df2.loc["adjustment", "fee_percentage"] = "Adjustment"
        df2.loc["adjustment", "admin_fee"] = adjustments["3.2"].adj
        df2.loc["total_after_adj", "fee_percentage"] = "Total After Adjustment"
        df2.loc["total_after_adj", "admin_fee"] = adjustments["3.2"].total_fee_after_adj
        df2[return_cols].to_excel(writer, sheet_name="3.2%", index=False)

        df_err = pd.DataFrame([x.dict() for x in error_rows])
        if error_rows:
            df_err["match"] = df_err["match"].astype(str)
            df_err.loc["total"] = df_err.sum(numeric_only=True)
            df_err.loc[df_err.index[-1], "fee_percentage"] = df_err[
                "fee_percentage"
            ].mean()

        df_err.to_excel(writer, sheet_name="ERROR_ROWS", index=False)

    return rows, error_rows, df1, df2, df_err


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        get_file = sys.argv[1]

    else:
        get_file = input("Enter file name: \n> ")
        assert get_file, "Please enter a file name"

    rows, error_rows, df1, df2, df_err = etl(get_file)
