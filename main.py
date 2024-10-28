import re
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class Column:

    def __init__(
        self,
        name: str = "unknown",
        data_type: str = "unknown",
        comment: Optional[str] = None,
    ):
        self.name: str = name
        self.data_type: str = data_type
        self.comment: Optional[str] = comment

    def __str__(self):
        return f"Column(name={self.name}, data_type={self.data_type}, comment={self.comment})"
    
    def __repr__(self) -> str:
        return self.__str__()


class Table:

    def __init__(
        self,
        catalog: str = "unknown",
        schema: str = "unknown",
        name: str = "unknown",
        columns: List[Column] = [],
        partition_by: Optional[List[str]] = None,
        location: Optional[str] = None,
        comment: Optional[str] = None,
        table_format: str = "delta",
    ):
        self.catalog: str = catalog
        self.schema: str = schema
        self.name: str = name
        self.columns: List[Column] = columns
        self.partition_by: Optional[List[str]] = partition_by
        self.location: Optional[str] = location
        self.comment: Optional[str] = comment
        self.table_format: str = table_format

    def apply_statement(self, statement: str):
        statement = statement.strip()
        statement = statement.replace(";", "")
        statement = statement.replace("\n", " ")
        statement = re.sub(r"\s+", " ", statement)
        statement = statement.lower()

        if statement.startswith("drop table"):
            return
        elif statement.startswith("create table"):
            self._create_table(statement)
        elif "add column" in statement:
            self._add_column(statement)
        elif "drop column" in statement:
            self._drop_column(statement)
    
    def _create_table(self, statement: str):
        pattern = r'create\s+table\s+([^\s(]+)\s*\((.*)\s*constraint.*\)'
        match = re.match(pattern, statement)
        if match:
            print(match.groups())
            self.catalog, self.schema, self.name = match.group(1).split(".")
            for col in match.group(2).split(","):
                col = col.strip()
                if len(col) == 0:
                    continue
                print(col)
                col_name, col_type, col_comment = re.match(r"([^\s]+)\s+([^\s]+)\s+comment\s+'(.*)'", col).groups()
                self.columns.append(Column(name=col_name, data_type=col_type, comment=col_comment))

    def _add_column(self, statement: str):
        idx = statement.find("add column")
        if idx == -1:
            return
        col = statement[idx+len("add column"):].strip()
        col_name, col_type, col_comment = re.match(r"([^\s]+)\s+([^\s]+)\s+comment\s+'(.*)'", col).groups()
        self.columns.append(Column(name=col_name, data_type=col_type, comment=col_comment))

    def _drop_column(self, statement: str):
        idx = statement.find("drop column")
        if idx == -1:
            return
        col = statement[idx+len("drop column"):].strip()
        self.columns = [c for c in self.columns if c.name != col]

    def __str__(self):
        return f"Table(catalog={self.catalog}, schema={self.schema}, name={self.name}, columns={self.columns}, partition_by={self.partition_by}, location={self.location}, comment={self.comment}, table_format={self.table_format})"


def parse_ddl_file(ddl_content: str) -> Optional[Table]:
    """Parse DDL file content and return final table state."""
    # Split content into individual statements
    statements = [s.strip() for s in ddl_content.split(";") if s.strip()]

    table = Table()

    for statement in statements:
        table.apply_statement(statement)
    return table


# Example usage
if __name__ == "__main__":
    with open("table_init.sql", "r") as f:
        ddl_content = f.read()

    table = parse_ddl_file(ddl_content)

    print(table)
