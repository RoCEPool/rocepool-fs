# Real-kernel FUSE mount test

This records an actual test session: `fs/fuse_lowlevel.py` (see that
file's module docstring for how it's built) mounted `PooledFileSystem`
(backed by `MemoryEngine` for this run) at a real mountpoint via a real
`mount(2)` syscall against `/dev/fuse`, and ordinary Linux tools drove it
through the kernel's actual VFS layer -- not a simulation, not a Python
dict standing in for a filesystem. This is the strongest verification
available in this sandbox for the "no FUSE kernel mount" gap called out
elsewhere in this project: it's now closed, for the in-memory backend, at
this scope.

## What this run does and doesn't prove

**Proves:** the FUSE kernel-protocol driver in `fs/fuse_lowlevel.py`
correctly implements enough of the real, stable kernel ABI
(`/usr/include/linux/fuse.h`, `FUSE_KERNEL_MINOR_VERSION 39` in this
sandbox) for the Linux kernel to accept the mount and route ordinary
POSIX operations through it correctly -- verified with real coreutils
(`mkdir`, `cat`, `cp`, `dd`, `truncate`, `rm`, `rmdir`, `find`, `df`) and
real Python file I/O (`open()`/`read()`/`write()`/`seek()`), including a
5MB binary round-trip checked by SHA-256 and a 70KB write forcing the
kernel to split it across multiple `WRITE` requests.

**Doesn't prove:** performance at any scale, multi-client concurrent
access correctness, hard-link/symlink/xattr/lock support (not
implemented -- see the module docstring for the exact op list), or
anything about the RapidsSE/WLP backend specifically -- this run used
`MemoryEngine`, not `RapidsSEEngine`. Swapping the backend
(`examples/mount_real_kernel_demo.py`'s docstring shows how) is
mechanical -- `fs/fuse_lowlevel.py` only depends on the `PooledFileSystem`
surface -- but wasn't re-run against `RapidsSEEngine` for this session.

## Environment

```
$ uname -a
Linux vm 6.18.44-fc-v42 #1 SMP PREEMPT_DYNAMIC @0 x86_64 x86_64 x86_64 GNU/Linux
$ id
uid=0(root) gid=0(root) groups=0(root)
$ ls -la /dev/fuse
crw------- 1 root root 10, 229 ... /dev/fuse
```
Root + `CAP_SYS_ADMIN` + `/dev/fuse` present, so the mount was done with
a direct `mount(2)` syscall (see `FuseKernelMount` in
`fs/fuse_lowlevel.py`) rather than the setuid `fusermount3` helper --
same kernel-facing result, one less moving part.

## What was actually run, and the result

```
$ mount | grep rocepool_test
rocepool on /mnt/rocepool_test type fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other,max_read=131072)

$ mkdir -p /mnt/rocepool_test/dir1/dir2
$ echo "hello real kernel" > /mnt/rocepool_test/dir1/hello.txt
$ cat /mnt/rocepool_test/dir1/hello.txt
hello real kernel

$ dd if=/dev/urandom of=/tmp/rand5m bs=1M count=5 2>/dev/null
$ cp /tmp/rand5m /mnt/rocepool_test/dir1/big.bin
$ sha256sum /tmp/rand5m /mnt/rocepool_test/dir1/big.bin
2e7614f0b3128605f31b69e31101addc6402ffa555dfed3c4ed096890635df08  /tmp/rand5m
2e7614f0b3128605f31b69e31101addc6402ffa555dfed3c4ed096890635df08  /mnt/rocepool_test/dir1/big.bin

$ echo "second line" >> /mnt/rocepool_test/dir1/hello.txt
$ cat /mnt/rocepool_test/dir1/hello.txt
hello real kernel
second line

$ truncate -s 5 /mnt/rocepool_test/dir1/hello.txt
$ stat -c '%s' /mnt/rocepool_test/dir1/hello.txt
5

$ cp /mnt/rocepool_test/dir1/hello.txt /mnt/rocepool_test/dir1/dir2/c.txt
$ rm /mnt/rocepool_test/dir1/dir2/c.txt
$ rmdir /mnt/rocepool_test/dir1/dir2

$ rmdir /mnt/rocepool_test/dir1
rmdir: failed to remove '/mnt/rocepool_test/dir1': Directory not empty   # correct: dir1 still has big.bin/hello.txt

$ df -h /mnt/rocepool_test
Filesystem      Size  Used Avail Use% Mounted on
rocepool        4.0G     0  4.0G   0% /mnt/rocepool_test

$ find /mnt/rocepool_test
/mnt/rocepool_test
/mnt/rocepool_test/dir1
/mnt/rocepool_test/dir1/big.bin
/mnt/rocepool_test/dir1/hello.txt

$ python3 -c "
path = '/mnt/rocepool_test/pyfile.bin'
with open(path, 'wb') as f: f.write(b'A' * 70000)   # forces multiple WRITE requests
with open(path, 'rb') as f: data = f.read()
print(len(data), data == b'A' * 70000)
with open(path, 'r+b') as f:
    f.seek(10); f.write(b'XYZ')
with open(path, 'rb') as f: print(f.read()[8:15])
"
70000 True
b'AAXYZAA'

$ mkdir -p /mnt/rocepool_test/a/b/c/d && echo x > /mnt/rocepool_test/a/b/c/d/leaf.txt
$ find /mnt/rocepool_test/a
/mnt/rocepool_test/a
/mnt/rocepool_test/a/b
/mnt/rocepool_test/a/b/c
/mnt/rocepool_test/a/b/c/d
/mnt/rocepool_test/a/b/c/d/leaf.txt

$ umount /mnt/rocepool_test
$ mount | grep rocepool_test   # (no output -- unmounted cleanly)
```

## A real bug this caught

The first attempt at `df -h` crashed the whole mount:

```
OSError: [Errno 22] Invalid argument
  File ".../fs/fuse_lowlevel.py", line ..., in _h_statfs
```

Cause: `struct fuse_kstatfs` (`linux/fuse.h`) is 5 `uint64_t` fields
followed by 4+6 `uint32_t` fields; the first version of `_h_statfs` here
packed it as 11 plain `uint32_t`s, which is the wrong size and field
widths, so the kernel rejected the reply outright and tore down the
mount. Fixed by matching the struct exactly
(`_STATFS_OUT = struct.Struct("<5Q4I6I")`), and a broad
`except Exception` was added around every request handler so a future
bug in one handler returns `EIO` for that one syscall instead of killing
the whole mount -- re-ran the entire sequence above afterward, including
`df -h`, with a clean daemon log throughout.

## Reproducing this yourself

```bash
python3 examples/mount_real_kernel_demo.py /mnt/some/empty/dir &
# use /mnt/some/empty/dir with any normal tool
umount /mnt/some/empty/dir
```
