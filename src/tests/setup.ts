import { afterEach, vi } from "vitest";

process.env = {
    ...process.env,
    DB_USER: "test_user",
};

vi.mock("oracledb");
vi.mock("fs");

afterEach(() => {
    vi.restoreAllMocks();
});
