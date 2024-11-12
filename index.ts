/*
  Customer states: Application generates a CSV export of personnel data;
  upon attempting to import this data to Microsoft SQL Server, data is
  corrupted; please diagnose and advise.

  CSV is formatted exactly as table is defined: (varchar, integer, varchar, varchar).
*/

/* CSV validations are not working as expected. Restructure the entire program to validate using headerTypeMap and make it flexible in case headers change.
1- Implemented schema object that directly maps each header to its expectedType to adjust headers and types without having to maintain separate definitions.
2- Implemented dynamic reading of the CSV headers to validate against the schema. It checks if headers in the CSV are recognized by the schema before processing
rows.
*/

/* Observations: Also noticed there was something wrong in the way my program is reading the CSV file.

1- Implemented line-by-line processing. Making the program more scalable, memory efficient and helpful when working with large CSV files.
2- Decided to remove the scan by characters since it accumulates characters making it difficult to identify type mismatches until an entire line is parsed. This previous implementation
makes handling quotes and delimiters too complex and can lead to issues if the CSV format varies.
3- Implemented helper functions (IsInteger, IsFloat, IsBoolean) to validate each field after the entire field is read. Better to verify type correctness. This
modular approach is helpful making the program easier to read, debug and extend with additional types if needed.
4- The program will log detailed errors messages each time a type mismatch is found. Including exact row, column, header and expected type. It also provides
a warning if any headers in the CSV file don't match the schema. Also made sure to have a standard format allowing for consistent reporting and easier troubleshooting of any 
potential validation issues in the CSV.
5- Direct splitting of lines into fields to simplify validation process. If the CSV have a straightforward structure (no nested quotes or complex delimiters) this approach is efficient
and reduces parsing complexity.

*/

import fs from "node:fs";
import readline from "node:readline";

type ConfigParameters = {
    delimiter: string;
    hasHeaders: boolean;
    separator: string;
    terminator: string;
};

type SchemaType = "string" | "integer" | "boolean" | "float";

const schema: { [key: string]: SchemaType } = {
    "name": "string",
    "age": "integer",
    "profession": "string",
    "gender": "string"
};

const config: ConfigParameters = JSON.parse(fs.readFileSync("config.json", "utf-8"));

function isInteger(value: string): boolean {
    return /^[1-7][0-9]?$/.test(value.trim());
}

function isFloat(value: string): boolean {
    return /^-?\d+(\.\d+)?$/.test(value.trim());
}

function isBoolean(value: string): boolean {
    const lowerVal = value.trim().toLowerCase();
    return ["true", "false", "1", "0", "yes", "no"].includes(lowerVal);
}

function validateType(value: string, expectedType: SchemaType): { isValid: boolean, errorMessage?: string } {
    const trimmedValue = value.trim();
    const maxLength = 50;

    switch (expectedType) {
        case "string":
            if (config.delimiter && !(trimmedValue.startsWith(config.delimiter) && trimmedValue.endsWith(config.delimiter))) {
                return { isValid: false, errorMessage: `Expected value to be enclosed in "${config.delimiter}" but got "${value}".` };
            }
            const unquotedString = trimmedValue.slice(1, -1);
            if (!/^[A-Za-z\s]+$/.test(unquotedString)) {
                return { isValid: false, errorMessage: `Expected alphabetic characters with spaces only, but got "${unquotedString}".` };
            }
            if (unquotedString.length > maxLength) {
                return { isValid: false, errorMessage: `String value "${unquotedString}" exceeds the maximum length of ${maxLength} characters.` };
            }
            return { isValid: true };

        case "integer":
            if (config.delimiter && !(trimmedValue.startsWith(config.delimiter) && trimmedValue.endsWith(config.delimiter))) {
                return { isValid: false, errorMessage: `Expected integer value to be enclosed in "${config.delimiter}" but got "${value}".` };
            }
            const unquotedInteger = trimmedValue.slice(1, -1);
            if (!isInteger(unquotedInteger)) {
                let errorMessage = `Expected an integer between 1 and 79, but got "${value}".`;
                if (/^-?\d+$/.test(unquotedInteger)) {
                    if (parseInt(unquotedInteger) > 99) {
                        errorMessage = `Integer value "${value}" exceeds the maximum accepted value (79). Accepted range is 1-79.`;
                    } else if (parseInt(unquotedInteger) <= 0) {
                        errorMessage = `Integer value "${value}" is below the minimum accepted value (1). Accepted range is 1-79.`;
                    }
                }
                return { isValid: false, errorMessage };
            }
            return { isValid: true };

        case "boolean":
            return { isValid: isBoolean(trimmedValue) };

        case "float":
            return { isValid: isFloat(trimmedValue) };

        default:
            return { isValid: false, errorMessage: `Unsupported type: ${expectedType}` };
    }
}

async function validateCSV(filePath: string) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let headers: string[] = [];
    let rowIndex = 0;

    for await (const line of rl) {
        const trimmedLine = line.trim();

        if (trimmedLine.startsWith(config.separator)) {
            console.error(`Error at row ${rowIndex + 1}: Expected a delimiter "${config.delimiter}" but got a separator "${config.separator}" at the beginning of the row.`);
            return;
        }

        if (trimmedLine.includes(config.separator + config.separator)) {
            console.error(`Error at row ${rowIndex + 1}: Expected a value or delimiter "${config.delimiter}" but got consecutive separators "${config.separator}${config.separator}".`);
            return;
        }

        if (trimmedLine.endsWith(config.separator)) {
            console.error(`Error at row ${rowIndex + 1}: Expected the terminator "${config.terminator}" but got a separator "${config.separator}" instead at the end of the row.`);
            return;
        }

        const fields = line.split(config.separator).map(field => field.trim());

        if (rowIndex === 0 && config.hasHeaders) {
            headers = fields;
            if (!headers.every(header => schema[header] !== undefined)) {
                console.error("Error: Unrecognized headers in the CSV file.");
                return;
            }
            rowIndex++;
            continue;
        }

        if (fields.length > headers.length) {
            console.error(`Error at row ${rowIndex + 1}: Extra columns detected. Expected ${headers.length} columns, but got ${fields.length}.`);
            return;
        } else if (fields.length < headers.length) {
            console.error(`Error at row ${rowIndex + 1}: Missing columns. Expected ${headers.length} columns, but got ${fields.length}.`);
            return;
        }

        for (let i = 0; i < fields.length; i++) {
            const header = headers[i];
            const expectedType = schema[header];
            const value = fields[i];

            const { isValid, errorMessage } = validateType(value, expectedType);
            if (!isValid) {
                console.error(`Type error at row ${rowIndex + 1}, column ${i + 1} ("${header}"): ${errorMessage}`);
            }
        }
        rowIndex++;
    }

    console.log("CSV validation completed.");
}

const filePath = process.argv[2];
if (!filePath) {
    console.error("Please provide the path to the CSV file.");
} else {
    validateCSV(filePath);
}