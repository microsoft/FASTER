
#include "debug.h"

#include <iostream>

#ifdef _WIN32
#include <Windows.h>
#include <dbghelp.h>
#endif

void print_backtrace() {
    std::ostream& out = std::cout;

#ifdef _WIN32
    const size_t levels = 62; // does not support more then 62 levels of stacktrace

    HANDLE process = GetCurrentProcess();

    out << "+++++Backtrace process: " << std::endl;
    SymInitialize(process, nullptr, TRUE);

    void* stack[levels];
    unsigned short frames = CaptureStackBackTrace(0, levels, stack, nullptr);

    out << "+++++Backtrace frames: " <<  frames << std::endl;

    SYMBOL_INFO* symbol = (SYMBOL_INFO*) calloc(sizeof(SYMBOL_INFO) + 256 * sizeof(char), 1);
    symbol->MaxNameLen = 255;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

    for(unsigned int i = 0; i < frames; i++) {
        SymFromAddr(process, (DWORD64)(stack[i]), 0, symbol);
        out << (frames - i - 1) << ": " << symbol->Name << " - " << symbol->Address << std::endl;
    }
    free(symbol);
#else
    out << "[debug] backtrace not available" << std::endl;
#endif
}
