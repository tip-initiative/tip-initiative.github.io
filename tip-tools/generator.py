import ast
import pickle
import re
import sys
import tempfile
from contextlib import suppress
from copy import copy
from pathlib import Path
from typing import List, Any, Union, Dict, Iterable

import attr
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import (
    SingleQuotedScalarString,
    FoldedScalarString,
    LiteralScalarString,
)

# Maximum width of line in YAML output
PAGE_WIDTH = 99

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
# DRAFT: SHEET_ID = "1J1v6ol6hSEWSlUPF4ZPMq-TMc_See3ZprvwjS_HqZic"
SHEET_ID = "15Y3G_RdBfnkXNdhbrYCPa0Cg0FMkCK6-vebIRhALPjg"

# Directory where the schemas live.
SCHEMAS = Path(__file__).absolute().parents[1] / "tip-initiative-apis" / "endpoints" / "schemas"

VERSION: str = "5.0.0"


@attr.s(frozen=True, slots=True)
class Sheet:
    schema: Path = attr.ib()
    name: str = attr.ib()
    start_row: int = attr.ib()
    title: str = attr.ib()
    description: str = attr.ib()

    @property
    def range(self) -> str:
        """Pull data from this Sheet range.

        We start at start_row and only go through column F.
        """
        return f"{self.name}!A:F"


def clean_description(description: str):
    """Remove duplicate / unwanted extra spaces from description."""
    rtype = str
    lines = []
    for line in description.splitlines():
        line = line.strip()
        if not line:
            continue
        r = " ".join(line.split())
        r = re.sub(r"\(\s+", "(", r)
        r = re.sub(r"\s+\)", ")", r)
        if len(r) > PAGE_WIDTH:
            rtype = FoldedScalarString
        lines.append(r)
    if len(lines) > 1:
        rtype = LiteralScalarString
    return rtype("\n".join(lines))


@attr.s(frozen=False, slots=True)
class ModelRow:
    name: str = attr.ib()
    required: str = attr.ib()
    type: str = attr.ib()
    data_type: str = attr.ib(factory=str)
    enum_values: str = attr.ib(factory=str)
    description: str = attr.ib(factory=str, converter=clean_description)
    sheet: Sheet = attr.ib(default=None)

    @classmethod
    def from_list(cls, row: List[str]):
        return cls(*[n.strip() for n in row])

    @property
    def is_required(self) -> bool:
        return self.required == "Required"

    @property
    def is_deleted(self) -> bool:
        return self.required.upper() == "DELETED"

    def parse(self, line_num: int = 0) -> Any:
        data_type = self.data_type.strip().lower()

        assert "array" not in data_type, self
        assert self.data_type, f"{line_num + 1}: {self}"

        if "string" == data_type:
            r = self.parse_string()

        elif "enum" == data_type:
            r = self.parse_enum()

        elif "integer" == data_type:
            r = self.parse_int()

        elif "date" == data_type:
            r = self.parse_date()

        elif "time" == data_type:
            r = self.parse_time()

        elif "date-time" == data_type:
            r = self.parse_datetime()

        elif "float" == data_type:
            r = self.parse_float()

        elif "double" == data_type:
            r = self.parse_double()

        elif "boolean" == data_type:
            r = self.parse_bool()

        elif "email" == data_type:
            r = self.parse_email()

        elif data_type.startswith("enum"):
            r = self.parse_enum()

        elif "," in data_type:
            r = self.parse_many()

        else:
            r = self.parse_ref()

        if "array" in self.type.lower():
            try:
                real_type = {"type": r["type"]}
            except KeyError:
                real_type = {"$ref": r["$ref"]}
            r = {"type": "array", "items": real_type}
            if constraints := self.enum_values.strip():
                vals = ast.literal_eval(constraints)
                r.update(vals)

        rr = {}

        if self.description:
            rr["description"] = self.description

        rr.update(r)

        return {self.name: rr}

    def parse_ref(self) -> dict:
        self.description = ""
        prefix = "#" if self.sheet.name == "Common Schemas" else "commonSchemas.yaml#"
        ref = f"{prefix}/components/schemas/{self.data_type.strip()}"
        return {"$ref": SingleQuotedScalarString(ref)}

    @staticmethod
    def parse_string() -> dict:
        return {"type": "string"}

    def parse_enum(self):
        vals = [n.strip() for n in self.enum_values.split(",")]
        return {"type": "string", "enum": vals}

    def parse_int(self):
        r = {"type": "integer"}
        if constraints := self.enum_values.strip():
            vals = ast.literal_eval(constraints)
            r.update(vals)
        return r

    @staticmethod
    def parse_date():
        return {"type": "string", "format": "date"}

    @staticmethod
    def parse_time():
        return {"type": "string", "pattern": "^(([0-1][0-9])|(2[0-3]))(:[0-5][0-9]){2}$"}

    @staticmethod
    def parse_datetime():
        return {"type": "string", "format": "date-time"}

    def parse_float(self):
        r = {"type": "number", "format": "float"}
        if constraints := self.enum_values.strip():
            vals = ast.literal_eval(constraints)
            r.update(vals)
        return r

    @staticmethod
    def parse_double():
        return {"type": "number", "format": "double"}

    @staticmethod
    def parse_bool():
        return {"type": "boolean"}

    @staticmethod
    def parse_email():
        return {"type": "string", "format": "email"}

    def parse_many(self):
        types = []
        for v in [n.strip() for n in self.data_type.split(",")]:
            other = copy(self)
            other.data_type = v
            other.description = ""
            r = other.parse()
            types.append(r[other.name])
        return {"oneOf": types}


# We use the "complicated" method of construction here because it makes dict keep the insert order,
# whereas we would get any-order-is-fine behavior with traditional construct.
def header(title: str, description: str) -> dict:
    version = VERSION
    return dict(
        [
            ("openapi", "3.0.0"),
            (
                "info",
                dict(
                    [
                        ("version", version),
                        ("title", title),
                        ("description", description),
                        ("termsOfService", "http://placeholderdomain.io/terms/"),
                        (
                            "contact",
                            dict(
                                [
                                    ("name", "TIP Initiative"),
                                    ("email", "tipinitiative@frontrowadvisory.com"),
                                    ("url", "http://placeholderdomain.io"),
                                ]
                            ),
                        ),
                        (
                            "license",
                            dict(
                                [("name", "MIT"), ("url", "https://opensource.org/licenses/MIT")]
                            ),
                        ),
                    ]
                ),
            ),
            ("paths", {}),
        ]
    )


def google_creds():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    token_pickle = Path(__file__).with_name("token.pickle")
    if token_pickle.exists():
        with token_pickle.open("rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            creds_json = Path(__file__).with_name("credentials.json")
            flow = InstalledAppFlow.from_client_secrets_file(creds_json, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with token_pickle.open("wb") as token:
            pickle.dump(creds, token)
    return creds


def main(sheets: Iterable[Sheet]):
    service = build("sheets", "v4", credentials=google_creds())

    sheet: Sheet
    for sheet in sheets:

        # Call the Sheets API
        svc = service.spreadsheets()
        result = svc.values().get(spreadsheetId=SHEET_ID, range=sheet.range).execute()
        values = result.get("values", [])
        if not values:
            return

        schemas: Dict[str, Dict[str, Union[List[str], Dict[str]]]] = {}
        klass: str = ""

        purpose = ""
        c: int
        v: List[str]
        for c, v in enumerate(values):
            row_num = c + 1

            # Sheet Purpose is the description for the top level Request Object.
            if row_num == 2:
                with suppress(IndexError):
                    if v[0].strip().lower() == "purpose":
                        purpose = clean_description(v[1])
                continue

            # Don't start reading until we hit the important stuff
            if row_num < sheet.start_row:
                continue

            # Skip the row if the "name" field is empty
            try:
                row: ModelRow = ModelRow.from_list(v)
                row.sheet = sheet
            except TypeError:
                print(f"WARNING: Stopped at [{sheet.name} -> Row {row_num}]: {v}")
                break

            if not row.description and purpose and row_num == sheet.start_row:
                row.description = purpose

            if row.type == "TypeDef":
                klass = row.name
                schemas[klass] = {"properties": {}}

                if row.data_type.lower() == "enum":
                    r = row.parse_enum()
                    schemas[klass] = r

                # Add description to new TypeDef. Gymnastics here to preserve order of fields.
                if row.description:
                    _obj = {"description": row.description}
                    _obj.update(schemas[klass])
                    schemas[klass] = _obj
                    del _obj

                continue

            if not klass:
                continue

            r = row.parse(c)
            if r:
                if row.is_deleted:
                    print(
                        f"INFO: {row.name} at [{sheet.name} -> Row {row_num}] is marked DELETED."
                    )
                    continue

                schemas[klass]["properties"].update(r)

                if row.is_required:
                    try:
                        schemas[klass]["required"].append(row.name)
                    except KeyError:
                        schemas[klass]["required"] = [row.name]

        content = header(sheet.title, sheet.description)
        content["components"] = {"schemas": schemas}

        sheet.schema.parent.mkdir(parents=True, exist_ok=True)
        with sheet.schema.open(mode="wt") as fp:
            with YAML(output=fp) as yaml:
                yaml.indent(mapping=2, sequence=4, offset=2)
                yaml.width = PAGE_WIDTH
                yaml.dump(content)
                print(f"INFO: Updated {sheet.schema}")


def re_dump(file_path: Union[Path, str]):
    """Open and reformat an existing YAML file."""
    file_: Path = Path(file_path)
    xyz = YAML(typ="safe", pure=True).load(file_.open().read())

    with YAML(output=sys.stdout) as yaml:
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.width = PAGE_WIDTH
        yaml.dump(xyz)


def other_main():
    with tempfile.TemporaryDirectory() as tmpdirname:
        temp_path = Path(tmpdirname)

        sheets: List[Sheet] = [
            Sheet(
                schema=SCHEMAS / "commonSchemas.yaml",
                name="Common Schemas",
                start_row=6,
                title="Common Schemas",
                description="Common Schemas",
            ),
            Sheet(
                schema=SCHEMAS / "logTimesSchemas.yaml",
                name="/seller/logtimes",
                start_row=11,
                title="logTimes Schemas",
                description="logTimes Schemas",
            ),
            Sheet(
                schema=temp_path / "buyer/inventoryAvailsSchemas.yaml",
                name="/buyer/inventoryAvails",
                start_row=11,
                title="Inventory Avails Schemas",
                description="Inventory Avails Schemas",
            ),
            Sheet(
                schema=temp_path / "seller/inventoryAvailsSchemas.yaml",
                name="/seller/inventoryAvails",
                start_row=11,
                title="Inventory Avails Schemas",
                description="Inventory Avails Schemas",
            ),
            Sheet(
                schema=SCHEMAS / "invoiceSchemas.yaml",
                name="/seller/invoice",
                start_row=11,
                title="Invoice Schema",
                description="Invoice Schema",
            ),
            Sheet(
                schema=SCHEMAS / "commercialInstructionSchemas.yaml",
                name="/buyer/commercialInstructions",
                start_row=11,
                title="Commercial Instructions Schema",
                description="Commercial Instructions Schema",
            ),
            Sheet(
                schema=SCHEMAS / "rfpsSchemas.yaml",
                name="/buyer/RFP",
                start_row=11,
                title="RFP Schema",
                description="RFP Schema",
            ),
            Sheet(
                schema=SCHEMAS / "proposalSchemas.yaml",
                name="/seller/proposal",
                start_row=11,
                title="Create a proposal to send to the buyer system",
                description="Seller/Proposal Schemas",
            ),
            Sheet(
                schema=temp_path / "buyer/ordersSchemas.yaml",
                name="/buyer/order",
                start_row=11,
                title="Buyer requesting New Order or Order Change Request to Seller",
                description="Buyer/Order",
            ),
            Sheet(
                schema=temp_path / "buyer/ordersSchemas.yaml",
                name="/buyer/order",
                start_row=11,
                title="Buyer requesting New Order or Order Change Request to Seller",
                description="Buyer/Order",
            ),
            Sheet(
                schema=temp_path / "seller/orderConfirmation.yaml",
                name="/seller/orderConfirmation",
                start_row=11,
                title="",
                description="",
            ),
            Sheet(
                schema=temp_path / "buyer/orderRecall.yaml",
                name="/buyer/orderRecall",
                start_row=11,
                title="",
                description="",
            ),
            Sheet(
                schema=temp_path / "seller/orderReject.yaml",
                name="/seller/orderReject",
                start_row=11,
                title="",
                description="",
            ),
        ]

        main(sheets)

        # Post combine Orders.
        # TODO: You're gonna have to do some manual work for now.
        out_file = SCHEMAS / "ordersSchemas.yaml"
        with out_file.open(mode="wb") as ofp:
            for n in [
                "buyer/ordersSchemas.yaml",
                "seller/orderConfirmation.yaml",
                "buyer/orderRecall.yaml",
                "seller/orderReject.yaml",
            ]:
                with (temp_path / n).open("rb") as ifp:
                    ofp.write(ifp.read())

        # Post combine InventoryAvails
        out_file = SCHEMAS / "inventoryAvailsSchemas.yaml"
        with out_file.open(mode="wb") as ofp:
            for n in [
                "buyer/inventoryAvailsSchemas.yaml",
                "seller/inventoryAvailsSchemas.yaml",
            ]:
                with (temp_path / n).open("rb") as ifp:
                    ofp.write(ifp.read())


if __name__ == "__main__":
    other_main()
