import { Dialect, Kysely } from "kysely";
import { ExecuteOptions, Pool } from "oracledb";
import { Options as PrettierOptions } from "prettier";
import { OracleAdapter } from "./adapter.js";
import { OracleDriver } from "./driver.js";
import { IntropsectorDB, OracleIntrospector } from "./introspector.js";
import { Logger } from "./logger.js";
import { OracleQueryCompiler } from "./query-compiler.js";

export interface OracleDialectConfig {
    pool: Pool;
    logger?: Logger;
    generator?: {
        type?: "tables" | "views" | "all";
        schemas?: string[];
        tables?: string[];
        views?: string[];
        camelCase?: boolean;
        checkDiff?: boolean;
        metadata?: boolean;
        filePath?: string;
        metadataFilePath?: string;
        prettierOptions?: PrettierOptions;
    };
    executeOptions?: ExecuteOptions;
}

export class OracleDialect implements Dialect {
    readonly #config: OracleDialectConfig;

    constructor(config: OracleDialectConfig) {
        this.#config = config;
    }

    createDriver(): OracleDriver {
        return new OracleDriver(this.#config);
    }

    createAdapter(): OracleAdapter {
        return new OracleAdapter();
    }

    createIntrospector(db: Kysely<IntropsectorDB>): OracleIntrospector {
        return new OracleIntrospector(db, this.#config);
    }

    createQueryCompiler(): OracleQueryCompiler {
        return new OracleQueryCompiler();
    }
}
