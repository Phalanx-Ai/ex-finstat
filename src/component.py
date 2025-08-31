"""
Finstat Keboola Component - Modernized Template
"""
import csv
import logging
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from finstat.client import FinstatClient

# Constants
API_LIMIT = 5000
KEY_TIMESTAMP = "timestamp"

DETAIL = [
    "Ico", "RegisterNumberText", "Dic", "IcDPH", "Name", "Street", "StreetNumber", "ZipCode", "City", "District",
    "Region", "Country", "Activity", "Created", "Cancelled", "SuspendedAsPerson", "Url", "Warning", "WarningUrl",
    "PaymentOrderWarning", "PaymentOrderUrl", "OrChange", "OrChangeUrl", "Revenue", "SkNaceCode", "SkNaceText",
    "SkNaceDivision", "SkNaceGroup", "LegalFormCode", "LegalFormText", "RpvsInsert", "RpvsUrl", "ProfitActual",
    "RevenueActual", "JudgementFinstatLink", "SalesCategory", "HasKaR", "KarUrl", "HasDebt", "DebtUrl",
    "JudgementIndicators"
]

EXTENDED = [
    "Ico", "Dic", "IcDPH", "Name", "Street", "StreetNumber", "ZipCode", "City", "Activity", "District",
    "Region", "Country", "Created", "Cancelled", "SuspendedAsPerson", "Url", "RegisterNumberText", "IcDphAdditional",
    "SkNaceCode", "SkNaceText", "SkNaceDivision", "SkNaceGroup", "Phones", "Emails", "Warning", "WarningUrl", "Debts",
    "StateReceivables", "CommercialReceivables", "PaymentOrderWarning", "PaymentOrderUrl", "PaymentOrders", "OrChange",
    "OrChangeUrl", "EmployeeCode", "EmployeeText", "LegalFormCode", "LegalFormText", "RpvsInsert", "RpvsUrl",
    "OwnershipTypeCode", "OwnershipTypeText", "CreditScoreValue", "ProfitActual", "ProfitPrev", "RevenueActual",
    "RevenuePrev", "ActualYear", "CreditScoreState", "ForeignResources", "GrossMargin", "ROA", "WarningLiquidation",
    "SelfEmployed", "WarningKaR", "Offices", "Subjects", "StructuredName", "HasKaR", "KarUrl", "HasDebt", "DebtUrl",
    "HasDisposal", "DisposalUrl", "ContactSources", "BasicCapital", "JudgementIndicators", "JudgementFinstatLink",
    "JudgementCounts", "JudgementLastPublishedDate", "Ratios", "SalesCategory"
]


class Component(ComponentBase):

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

        # select requested schema
        if params.request_type == "detail":
            request_cols = DETAIL
        elif params.request_type == "extended":
            request_cols = EXTENDED
        else:
            raise UserException(f"Unsupported request_type: {params.request_type}")

        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables provided")

        input_file = input_tables[0].full_path
        input_icos = self._get_input_icos(input_file, params.ico_field)
        client = FinstatClient(params.api_key, params.private_key, params.request_type)

        timestamp = datetime.now().isoformat()
        results, bad_icos = self._get_results(client, input_icos, request_cols, timestamp)

        # add timestamp column
        request_cols_with_ts = request_cols + [KEY_TIMESTAMP]

        # good results
        self._write_results(
            results,
            request_type=params.request_type,
            columns=request_cols_with_ts,
            filename_suffix=""
        )

        # bad ICOs
        self._write_results(
            bad_icos,
            request_type=params.request_type,
            columns=["Ico"],
            filename_suffix="_bad_icos",
            incremental=False
        )

    def _get_input_icos(self, input_file, ico_field):
        icos = []
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                icos.append(row[ico_field])

        if len(icos) > API_LIMIT:
            logging.warning(f"More than {API_LIMIT} ICOs requested, truncating to {API_LIMIT}")
            return icos[:API_LIMIT]
        return icos

    def _get_results(self, client, input_icos, request_cols, timestamp):
        results, bad_icos = [], []
        for ico in input_icos:
            result = client.get_ico_data(ico)
            if result:
                normalized = {col: result.get(col, "") for col in request_cols}
                normalized[KEY_TIMESTAMP] = timestamp
                results.append(normalized)
            else:
                bad_icos.append({"Ico": ico})
        return results, bad_icos

    def _write_results(self, results, request_type, columns, filename_suffix="", incremental=True):
        filename = f"finstat_{request_type}{filename_suffix}.csv"
        table = self.create_out_table_definition(filename, incremental=incremental, primary_key=["Ico"])
        table.columns = columns

        with open(table.full_path, mode="wt", encoding="utf-8", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=columns)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
