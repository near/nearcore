
# compile with
# make QEMU_DIR=/host/qemu && cp libcounter.so /host/qemu-linux/plugins/
# run with
# /host/qemu-linux/bin/qemu-x86_64 -d plugin -plugin file=/host/qemu-linux/plugins/libcounter.so,arg="on_every_close"  /bin/ls

CFLAGS += -fPIC
CFLAGS += -I$(QEMU_DIR)/include/qemu -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include

all: libcounter.so

%o.: %.c
	$(CC) $(CFLAGS) -fPIC $^ -o $@

libcounter.so: counter.o
	$(CC) -shared -Wl,-soname,$@ -o $@ $^

test_binary: test.o
	$(CC) $(CFLAGS) $^ -o $@ -lpthread

test: test_binary
	/host/qemu-linux/bin/qemu-x86_64 -d plugin -plugin file=/host/qemu-linux/plugins/libcounter.so ./test_binary

clean:
	rm -f *.o *.so ./test_binary

.PHONY: all clean
