import { Kysely } from "kysely";
import oracledb from "oracledb";
import { OracleDialect } from "../src/dialect/dialect";
oracledb.initOracleClient({ libDir: String.raw`C:\Users\lones\Desktop\instantclient-basic-windows.x64-23.7.0.25.01\instantclient_23_7` })

async function main() {
    const dialect = new OracleDialect({
        pool: await oracledb.createPool({
            user: "ZSYF",
            password: "ZSYF123",
            connectString: "172.16.0.15:1521/qazhis"
        }),
    });
    const db = new Kysely({ dialect });
    const result = await db.selectFrom("P_XML_SY_VIEW" as any).selectAll().where("rownum" as any, "<", 10).stream()
    for await (const element of result) {
        console.log(element)
    }
    // console.log(result)
}
main()