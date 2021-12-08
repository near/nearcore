/* This file is in order to get a common clock resolution on possible host, i.e. Linux and Mac. */
#include <stdio.h>
#include <time.h>

int main() {
    struct timespec res;
    struct timespec time;

    clock_getres(CLOCK_MONOTONIC, &res);
    clock_gettime(CLOCK_MONOTONIC, &time);

    printf("CLOCK_MONOTONIC: res.tv_sec=%lu res.tv_nsec=%lu\n", res.tv_sec, res.tv_nsec);
    printf("CLOCK_MONOTONIC: time.tv_sec=%lu time.tv_nsec=%lu\n", time.tv_sec, time.tv_nsec);

    clock_getres(CLOCK_PROCESS_CPUTIME_ID, &res);
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time);

    printf("CLOCK_PROCESS_CPUTIME_ID: res.tv_sec=%lu res.tv_nsec=%lu\n", res.tv_sec, res.tv_nsec);
    printf("CLOCK_PROCESS_CPUTIME_ID: time.tv_sec=%lu time.tv_nsec=%lu\n", time.tv_sec, time.tv_nsec);

    clock_getres(CLOCK_REALTIME, &res);
    clock_gettime(CLOCK_REALTIME, &time);

    printf("CLOCK_REALTIME: res.tv_sec=%lu res.tv_nsec=%lu\n", res.tv_sec, res.tv_nsec);
    printf("CLOCK_REALTIME: time.tv_sec=%lu time.tv_nsec=%lu\n", time.tv_sec, time.tv_nsec);

    clock_getres(CLOCK_THREAD_CPUTIME_ID, &res);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time);

    printf("CLOCK_THREAD_CPUTIME_ID: res.tv_sec=%lu res.tv_nsec=%lu\n", res.tv_sec, res.tv_nsec);
    printf("CLOCK_THREAD_CPUTIME_ID: time.tv_sec=%lu time.tv_nsec=%lu\n", time.tv_sec, time.tv_nsec);
}

