#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#define CATCH_BASE 0xcafebabe
#define THREAD_NUM 10

static void start_counting() {
    char buf;
    int rv = read(CATCH_BASE, &buf, 1);
    (void)rv;
    fprintf(stderr, "start counting\n");
}

static void end_counting() {
    uint64_t counter = 0;
    fprintf(stderr, "end counting\n");
    int rv = read(CATCH_BASE + 1, &counter, sizeof(counter));
    (void)rv;
    printf("We got %lld from TCG\n", counter);
    counter = 0;
    rv = read(CATCH_BASE + 2, &counter, sizeof(counter));
    (void)rv;
    printf("%lld bytes read\n", counter);
    counter = 0;
    rv = read(CATCH_BASE + 3, &counter, sizeof(counter));
    (void)rv;
    printf("%lld bytes written\n", counter);
}

int global = 0;
volatile int started = 0;
volatile int running = 0;

typedef struct {
    int delay;
} ThreadArg;

int g_fd = 0;
ssize_t g_file_size = 0;

static void* thread_fn(void* varg)  {
    ThreadArg* arg = varg;
    int i;
    ssize_t rv;
    char data[1024];
    __sync_fetch_and_add(&started, 1);
    while (__sync_fetch_and_add(&started, 0) != THREAD_NUM + 1) {}

    for (i = 0; i < arg->delay * 1000; i++) {
        global += i;
    }
    rv = pread(g_fd, data, sizeof(data), arg->delay * 1000);
    fprintf(stderr, "rv1=%d\n", (int)rv);
    rv = pwrite(g_fd, data, sizeof(data), arg->delay * 2000 + 4096);
    fprintf(stderr, "rv2=%d\n", (int)rv);
    free(arg);
    return NULL;
}

int main(int argc, char** argv) {
    int i;
    int repeat = 100;
    pthread_t threads[THREAD_NUM];
    char block[4096];

    if (argc > 1) {
        repeat = atoi(argv[1]);
    }

    g_fd = open("/tmp/data_file", O_RDWR | O_CREAT);
    memset(block, 0xff, sizeof(block));
    for (i = 0; i < 1000; i++) {
        g_file_size += write(g_fd, block, sizeof(block));
    }

    for (i = 0; i < THREAD_NUM; i++) {
        ThreadArg* arg = calloc(sizeof(ThreadArg), 1);
        arg->delay = i * 100;
        pthread_create(threads + i, NULL, thread_fn, arg);
    }

    while (__sync_fetch_and_add(&started, 0) < THREAD_NUM) {}
    start_counting();
    __sync_fetch_and_add(&started, 1);

    for (i = 0; i < repeat; i++) {
        global += i;
    }

    for (i = 0; i < THREAD_NUM; i++) {
        pthread_join(threads[i], NULL);
    }

    close(g_fd);

    end_counting();

    return 0;
}