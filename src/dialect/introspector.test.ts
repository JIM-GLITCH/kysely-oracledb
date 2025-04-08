import { Kysely } from "kysely";
import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import { OracleDialect } from "./dialect";
import { IntropsectorDB } from "./introspector";

describe("OracleIntrospector", () => {
    it("returns schema metadata", async () => {
        const pool = await oracledb.createPool({
            user: process.env.DB_USER,
        });

        const mockedExecute = vi.fn(() => {
            return {
                rows: [{ username: "SYS" }],
                rowsAffected: 0,
            };
        });

        vi.spyOn(pool, "getConnection").mockImplementation(async () => {
            return {
                close: vi.fn(),
                execute: mockedExecute,
            };
        });

        const dialect = new OracleDialect({
            pool,
        });

        const db = new Kysely<IntropsectorDB>({ dialect });

        const intropsector = dialect.createIntrospector(db);

        const schemas = await intropsector.getSchemas();

        expect(mockedExecute).toHaveBeenCalledWith(
            "select username from allUsers where (:0 = :1 or username in (:2)) fetch next :3 rows only",
            [0, 0, null, 999],
            expect.anything(),
        );

        expect(schemas).toEqual([{ name: "SYS" }]);
    });
    it("returns table metadata", async () => {
        const pool = await oracledb.createPool({
            user: process.env.DB_USER,
        });

        const mockedExecute = vi
            .fn()
            .mockResolvedValueOnce({
                rows: [{ username: "SYS" }],
                rowsAffected: 0,
            })
            .mockResolvedValueOnce({
                rows: [
                    {
                        owner: "SYS",
                        tableName: "DUAL",
                    },
                ],
                rowsAffected: 0,
            })
            .mockResolvedValueOnce({
                rows: [
                    {
                        owner: "SYS",
                        tableName: "DUAL",
                        columnName: "DUMMY",
                        dataType: "VARCHAR2",
                        dataLength: 1,
                        dataPrecision: null,
                        dataScale: null,
                        nullable: "Y",
                        dataDefault: null,
                        identityColumn: null,
                    },
                ],
                rowsAffected: 0,
            });

        vi.spyOn(pool, "getConnection").mockImplementation(async () => {
            return {
                close: vi.fn(),
                execute: mockedExecute,
            };
        });

        const dialect = new OracleDialect({
            pool,
        });

        const db = new Kysely<IntropsectorDB>({ dialect });

        const intropsector = dialect.createIntrospector(db);

        const tables = await intropsector.getTables();

        expect(mockedExecute).toHaveBeenNthCalledWith(
            1,
            "select username from allUsers where (:0 = :1 or username in (:2)) fetch next :3 rows only",
            [0, 0, null, 999],
            expect.anything(),
        );
        expect(mockedExecute).toHaveBeenNthCalledWith(
            2,
            "select owner, tableName from allTables where owner in (:0) and (:1 = :2 or tableName in (:3)) fetch next :4 rows only",
            ["SYS", 0, 0, null, 999],
            expect.anything(),
        );
        expect(mockedExecute).toHaveBeenNthCalledWith(
            3,
            "select owner, tableName, columnName, dataType, dataLength, dataPrecision, dataScale, nullable, dataDefault, identityColumn from allTabColumns where owner in (:0, :1) and tableName in (:2)",
            ["SYS", "SYS", "DUAL"],
            expect.anything(),
        );

        expect(tables).toEqual([
            {
                schema: "SYS",
                name: "DUAL",
                isView: false,
                columns: [
                    {
                        name: "DUMMY",
                        dataType: "VARCHAR2",
                        dataLength: 1,
                        dataPrecision: null,
                        dataScale: null,
                        isNullable: true,
                        hasDefaultValue: false,
                        isAutoIncrementing: false,
                    },
                ],
            },
        ]);
    });
    it("returns view metadata", async () => {
        const pool = await oracledb.createPool({
            user: process.env.DB_USER,
        });

        const mockedExecute = vi
            .fn()
            .mockResolvedValueOnce({
                rows: [{ username: "SYS" }],
                rowsAffected: 0,
            })
            .mockResolvedValueOnce({
                rows: [
                    {
                        owner: "SYS",
                        viewName: "DUAL",
                    },
                ],
                rowsAffected: 0,
            })
            .mockResolvedValueOnce({
                rows: [
                    {
                        owner: "SYS",
                        tableName: "DUAL",
                        columnName: "DUMMY",
                        dataType: "VARCHAR2",
                        nullable: "Y",
                        dataDefault: null,
                        identityColumn: null,
                    },
                ],
                rowsAffected: 0,
            });

        vi.spyOn(pool, "getConnection").mockImplementation(async () => {
            return {
                close: vi.fn(),
                execute: mockedExecute,
            };
        });

        const dialect = new OracleDialect({
            pool,
        });

        const db = new Kysely<IntropsectorDB>({ dialect });

        const intropsector = dialect.createIntrospector(db);

        const views = await intropsector.getViews();

        expect(mockedExecute).toHaveBeenNthCalledWith(
            1,
            "select username from allUsers where (:0 = :1 or username in (:2)) fetch next :3 rows only",
            [0, 0, null, 999],
            expect.anything(),
        );
        expect(mockedExecute).toHaveBeenNthCalledWith(
            2,
            "select owner, viewName from allViews where owner in (:0) and (:1 = :2 or viewName in (:3)) fetch next :4 rows only",
            ["SYS", 0, 0, null, 999],
            expect.anything(),
        );
        expect(mockedExecute).toHaveBeenNthCalledWith(
            3,
            "select owner, tableName, columnName, dataType, nullable, dataDefault, identityColumn from allTabColumns where owner in (:0) and tableName in (:1)",
            ["SYS", "DUAL"],
            expect.anything(),
        );

        expect(views).toEqual([
            {
                schema: "SYS",
                name: "DUAL",
                isView: true,
                columns: [
                    {
                        name: "DUMMY",
                        dataType: "VARCHAR2",
                        isNullable: true,
                        hasDefaultValue: false,
                        isAutoIncrementing: false,
                    },
                ],
            },
        ]);
    });
});
