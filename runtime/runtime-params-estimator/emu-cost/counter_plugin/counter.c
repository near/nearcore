#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <glib.h>

#include <qemu-plugin.h>

QEMU_PLUGIN_EXPORT int qemu_plugin_version = QEMU_PLUGIN_VERSION;

// Files with descriptors after this one are intercepted for instruction counting marks.
#define CATCH_BASE 0xcafebabe

static uint64_t insn_count = 0;
static pthread_t counting_for = 0;
static bool counting = false;
static bool count_per_thread = false;
static bool on_every_close = false;

static void vcpu_insn_exec_before(unsigned int cpu_index, void *udata)
{
    if (!counting) return;
    if (!count_per_thread || pthread_self() == counting_for)
        insn_count++;
}

static void vcpu_tb_trans(qemu_plugin_id_t id, struct qemu_plugin_tb *tb)
{
    size_t n = qemu_plugin_tb_n_insns(tb);
    size_t i;

    for (i = 0; i < n; i++) {
        struct qemu_plugin_insn *insn = qemu_plugin_tb_get_insn(tb, i);

        // TODO: do this call only on first insn in bb.
        qemu_plugin_register_vcpu_insn_exec_cb(
            insn, vcpu_insn_exec_before, QEMU_PLUGIN_CB_NO_REGS, NULL);
    }
}

static void print_insn_count(void) {
    g_autofree gchar *out = g_strdup_printf("executed %" PRIu64 " instructions\n", insn_count);
    qemu_plugin_outs(out);
}

static void vcpu_syscall(qemu_plugin_id_t id, unsigned int vcpu_index,
                        int64_t num, uint64_t a1, uint64_t a2,
                        uint64_t a3, uint64_t a4, uint64_t a5,
                        uint64_t a6, uint64_t a7, uint64_t a8)
{
    // We put our listener on fd reads in range [CATCH_BASE, CATCH_BASE + 1]
    if (num == 0) { // sys_read
        switch (a1)
        {
            case CATCH_BASE + 0:
                counting = true;
                if (count_per_thread)
                    counting_for = pthread_self();
                insn_count = 0;
                break;
            case CATCH_BASE + 1: {
                counting_for = 0;
                counting = false;
                // If read syscall passes buffer size (third syscall argument) equal to 8 we assume it is interested
                // in getting back the actual number of executed instructions.
                if (a3 == 8) {
                    // In case of user emulation in QEMU, addresses are 1:1 translated, so we can tell the caller
                    // number of executed instructions by just writing into the buffer argument of read.
                    *(uint64_t*)a2 = insn_count;
                }
                print_insn_count();
                break;
            }
            default:
                break;
        }
    }
    if (num == 3 && on_every_close) { // sys_close
        print_insn_count();
    }
}

QEMU_PLUGIN_EXPORT int qemu_plugin_install(qemu_plugin_id_t id,
                                           const qemu_info_t *info,
                                           int argc, char **argv)
{
    int i;
    for (i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "on_every_close")) {
            on_every_close = true;
            counting_for = pthread_self();
        }
        if (!strcmp(argv[i], "count_per_thread")) {
            qemu_plugin_outs("count per thread\n");
            count_per_thread = true;
            counting_for = pthread_self();
        }
    }

    qemu_plugin_register_vcpu_tb_trans_cb(id, vcpu_tb_trans);
    qemu_plugin_register_vcpu_syscall_cb(id, vcpu_syscall);
    return 0;
}
