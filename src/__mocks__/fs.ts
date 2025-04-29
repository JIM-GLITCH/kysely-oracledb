import { vi } from "vitest";

export const mockedWrite = vi.fn();
export const mockedRead = vi.fn();

export default {
    writeFileSync: mockedWrite,
    readFileSync: mockedRead,
};
