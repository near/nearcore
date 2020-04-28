#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

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
int started = 0;

typedef struct {
    int delay;
} ThreadArg;

static void* thread_fn(void* varg)  {
    ThreadArg* arg = varg;
    int i;
    __sync_fetch_and_add(&started, 1);
    for (i = 0; i < arg->delay * 1000; i++) {
        global += i;
    }
    free(arg);
    return NULL;
}

int main(int argc, char** argv) {
    int i;
    int repeat = 100;
#define THREAD_NUM 10
    pthread_t threads[THREAD_NUM];

    if (argc > 1) {
        repeat = atoi(argv[1]);
    }

    for (i = 0; i < THREAD_NUM; i++) {
        ThreadArg* arg = calloc(sizeof(ThreadArg), 1);
        arg->delay = i * 100;
        pthread_create(threads + i, NULL, thread_fn, arg);
    }

    while (__sync_fetch_and_add(&started, 0) < THREAD_NUM) {}

    start_counting();
    for (i = 0; i < repeat; i++) {
        global += i;
    }
    end_counting();

    for (i = 0; i < THREAD_NUM; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}