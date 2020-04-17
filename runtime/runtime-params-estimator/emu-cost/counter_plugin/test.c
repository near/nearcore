#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define CATCH_BASE 0xcafebabe

static void start_counting() {
    char buf;
    int rv = read(CATCH_BASE, &buf, 1);
    (void)rv;
}

static void end_counting() {
    uint64_t counter = 0;
    int rv = read(CATCH_BASE + 1, &counter, sizeof(counter));
    (void)rv;
    printf("We got %lld from TCG\n", counter);
}

int global = 0;

int main(int argc, char** argv) {
    int i;
    int repeat = 100;

    if (argc > 1) {
        repeat = atoi(argv[1]);
    }
    start_counting();
    for (i = 0; i < repeat; i++) {
        global += i;
    }
    end_counting();
    return 0;
}