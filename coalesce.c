/*
 * coalesce -- a Unix-style coalescing oneshot runner.
 *
 *   coalesce run     NAME -- COMMAND [ARG...]
 *   coalesce trigger NAME
 *   coalesce poke    NAME -- COMMAND [ARG...]
 *   coalesce status  NAME
 *   coalesce cancel  NAME
 *
 * State machine:
 *
 *   idle       + trigger -> run            (spawn the command)
 *   running    + trigger -> mark dirty
 *   running    + N triggers -> still only dirty
 *   finish     + dirty    -> run once more
 *   finish     + clean    -> idle
 *
 * `run` is a long-lived worker: it stays alive listening on a unix socket so
 * that command-less `trigger`s can reach it. `poke` spawns the worker if it
 * isn't already running, then triggers it -- the single-command integration
 * path for webhooks and file watchers.
 *
 * Build: make
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* protocol bytes */
enum { MSG_TRIGGER = 'T', MSG_CANCEL = 'C', MSG_STATUS = 'S' };

static void die(const char *msg) {
  perror(msg);
  exit(1);
}

static int valid_name(const char *name) {
  size_t n = strlen(name);
  if (n == 0 || n > 63) return 0;
  for (const char *p = name; *p; p++) {
    char c = *p;
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.'))
      return 0;
  }
  return 1;
}

/* Validate a name, reporting and returning 1 if it is rejected. */
static int check_name(const char *name) {
  if (valid_name(name)) return 0;
  fprintf(stderr, "coalesce: invalid name '%s'\n", name);
  return 1;
}

/* runtime directory: XDG_RUNTIME_DIR, /run (root), or /tmp/coalesce-<uid> */
static const char *runtimedir(void) {
  const char *e = getenv("XDG_RUNTIME_DIR");
  if (e && *e && e[0] == '/') return e;
  if (geteuid() == 0) return "/run";
  static char buf[PATH_MAX];
  snprintf(buf, sizeof buf, "/tmp/coalesce-%u", (unsigned)getuid());
  return buf;
}

static int make_dir(const char *p) {
  if (mkdir(p, 0700) < 0 && errno != EEXIST) return -1;
  return 0;
}

static int socket_path(const char *name, char *out, size_t outsz) {
  const char *base = runtimedir();
  char dir[PATH_MAX];
  int n = snprintf(dir, sizeof dir, "%s/coalesce", base);
  if (n < 0 || n >= (int)sizeof dir) return -1;
  if (make_dir(base) < 0 || make_dir(dir) < 0) return -1;
  n = snprintf(out, outsz, "%s/%s.sock", dir, name);
  if (n < 0 || n >= (int)outsz) return -1;
  return 0;
}

static void ux_addr(struct sockaddr_un *sa, const char *path) {
  memset(sa, 0, sizeof *sa);
  sa->sun_family = AF_UNIX;
  strncpy(sa->sun_path, path, sizeof sa->sun_path - 1);
}

static int ux_connect(const char *path) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  struct sockaddr_un sa;
  ux_addr(&sa, path);
  if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
    close(fd);
    return -1;
  }
  return fd;
}

static int set_nonblock(int fd) {
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl < 0) return -1;
  return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int set_cloexec(int fd) {
  int fl = fcntl(fd, F_GETFD, 0);
  if (fl < 0) return -1;
  return fcntl(fd, F_SETFD, fl | FD_CLOEXEC);
}

/* exchange one message byte for a state-line reply on an open fd */
static int exchange(int fd, char msg, char *st, size_t ssz) {
  if (write(fd, &msg, 1) != 1) {
    close(fd);
    return -1;
  }
  size_t off = 0;
  for (;;) {
    ssize_t r = read(fd, st + off, ssz - 1 - off);
    if (r <= 0) break;
    off += (size_t)r; /* r > 0 here */
    st[off] = 0;
    if (strchr(st, '\n')) break;
  }
  close(fd);
  st[off] = 0;
  if (off && st[off - 1] == '\n') st[off - 1] = 0;
  return 0;
}

/* Connect to a worker, send one message byte, and optionally print the
 * returned state line. Returns 0 on success, 1 if no worker is listening. */
static int client_cmd(const char *name, char msg, int do_print) {
  char path[PATH_MAX], st[64];
  if (socket_path(name, path, sizeof path) < 0) die("socket_path");
  int fd = ux_connect(path);
  if (fd < 0 || exchange(fd, msg, st, sizeof st) < 0) {
    fprintf(stderr, "coalesce: no worker '%s'\n", name);
    return 1;
  }
  if (do_print) printf("%s\n", st);
  return 0;
}

static int g_wake[2] = {-1, -1};
static volatile sig_atomic_t sig_chld = 0, sig_term = 0;

static void on_sig(int s) {
  int saved = errno;
  if (s == SIGCHLD) sig_chld = 1;
  else sig_term = 1;
  write(g_wake[1], "x", 1);
  errno = saved;
}

/* fork+exec the command, resolving exec synchronously. The error pipe's
 * write end is CLOEXEC: a successful exec closes it (parent read -> EOF), a
 * failed exec writes errno then exits (parent read -> bytes).
 * Returns 0 (child running), 1 (exec failed; reported and reaped), -1 (fork or
 * pipe failure). */
static int spawn(char **argv, pid_t *out_pid) {
  int p[2];
  if (pipe(p) < 0) return -1;
  pid_t pid = fork();
  if (pid < 0) {
    close(p[0]);
    close(p[1]);
    return -1;
  }
  if (pid == 0) {
    close(p[0]);
    fcntl(p[1], F_SETFD, FD_CLOEXEC);
    signal(SIGCHLD, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);
    execvp(argv[0], argv);
    int e = errno;
    write(p[1], &e, sizeof e);
    _exit(127);
  }
  close(p[1]);
  int e = EIO;
  ssize_t r = read(p[0], &e, sizeof e); /* blocks until exec resolves */
  close(p[0]);
  if (r > 0) {
    int st;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {
    }
    fprintf(stderr, "coalesce: exec '%s' failed: %s\n", argv[0], strerror(e));
    return 1;
  }
  if (r < 0) return -1;
  *out_pid = pid;
  return 0;
}

static const char *state_str(int running, int dirty) {
  if (!running) return "idle";
  return dirty ? "running dirty" : "running";
}

/* Spawn the command and update worker state. Returns 0 if started or the
 * failure was transient (logged), 2 if the command is unrunnable -- the
 * caller should unlink the socket and exit 2. */
static int start_run(char **argv, pid_t *child, int *running, int *dirty) {
  int r = spawn(argv, child);
  if (r == 0) {
    *running = 1;
    *dirty = 0;
  } else if (r == 1) {
    return 2; /* command is unrunnable */
  } else {
    perror("coalesce: spawn");
  }
  return 0;
}

static int run_worker(const char *name, const char *path, char **argv) {
  int lfd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (lfd < 0) die("socket");

  struct sockaddr_un sa;
  ux_addr(&sa, path);

  if (bind(lfd, (struct sockaddr *)&sa, sizeof sa) < 0) {
    if (errno != EADDRINUSE) die("bind");
    /* address in use: a live worker owns it, else the socket is stale. */
    int t = ux_connect(path);
    if (t >= 0) {
      close(t);
      fprintf(stderr, "coalesce: worker '%s' already running\n", name);
      return 1;
    }
    unlink(path);
    if (bind(lfd, (struct sockaddr *)&sa, sizeof sa) < 0) die("bind");
  }
  if (listen(lfd, 16) < 0) die("listen");
  set_nonblock(lfd);
  set_cloexec(lfd);

  if (pipe(g_wake) < 0) die("pipe");
  set_nonblock(g_wake[0]);
  set_nonblock(g_wake[1]);
  set_cloexec(g_wake[0]);
  set_cloexec(g_wake[1]);

  struct sigaction s;
  memset(&s, 0, sizeof s);
  s.sa_handler = on_sig;
  sigemptyset(&s.sa_mask);
  s.sa_flags = SA_RESTART;
  sigaction(SIGCHLD, &s, NULL);
  sigaction(SIGTERM, &s, NULL);
  sigaction(SIGINT, &s, NULL);
  signal(SIGPIPE, SIG_IGN);

  int running = 0, dirty = 0;
  pid_t child = -1;

  for (;;) {
    struct pollfd p[2] = {{lfd, POLLIN, 0}, {g_wake[0], POLLIN, 0}};
    if (poll(p, 2, -1) < 0) {
      if (errno == EINTR) continue;
      die("poll");
    }

    /* serve clients */
    if (p[0].revents & POLLIN) {
      for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) break;
        struct timeval tv = {.tv_sec = 2};
        setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        char msg = 0;
        /* Every client gets the state line back; MSG_STATUS is just a read
         * with no side effect, so only TRIGGER and CANCEL are handled here. */
        if (read(cfd, &msg, 1) == 1) {
          if (msg == MSG_TRIGGER) {
            if (!running) {
              if (start_run(argv, &child, &running, &dirty) == 2) {
                close(cfd);
                unlink(path);
                return 2;
              }
            } else {
              dirty = 1;
            }
          } else if (msg == MSG_CANCEL) {
            dirty = 0;
          }
        }
        char buf[64];
        int m = snprintf(buf, sizeof buf, "%s\n", state_str(running, dirty));
        write(cfd, buf, (size_t)m); /* m is a small positive snprintf result */
        close(cfd);
      }
    }

    /* signals */
    if (p[1].revents & POLLIN) {
      char tmp[64];
      while (read(g_wake[0], tmp, sizeof tmp) > 0) {
      }
      if (sig_term) {
        if (running && child > 0) {
          kill(child, SIGTERM);
          int st;
          while (waitpid(child, &st, 0) < 0 && errno == EINTR) {
          }
        }
        unlink(path);
        return 0;
      }
      if (sig_chld) {
        sig_chld = 0;
        int st;
        if (child > 0 && waitpid(child, &st, WNOHANG) == child) {
          running = 0;
          child = -1;
          if (dirty && start_run(argv, &child, &running, &dirty) == 2) {
            unlink(path);
            return 2;
          }
        }
      }
    }
  }
}

static int cmd_poke(const char *name, char **argv) {
  char path[PATH_MAX];
  if (socket_path(name, path, sizeof path) < 0) die("socket_path");

  int fd = ux_connect(path);
  if (fd < 0) {
    /* spawn a detached worker, then retry the connection. */
    pid_t pid = fork();
    if (pid < 0) die("fork");
    if (pid == 0) {
      setsid();
      int dn = open("/dev/null", O_RDWR);
      if (dn >= 0) {
        dup2(dn, 0);
        dup2(dn, 1);
        dup2(dn, 2);
        if (dn > 2) close(dn);
      }
      signal(SIGHUP, SIG_IGN);
      _exit(run_worker(name, path, argv));
    }
    for (int i = 0; i < 300; i++) {
      fd = ux_connect(path);
      if (fd >= 0) break;
      usleep(10 * 1000); /* 10ms; up to 3s */
    }
    if (fd < 0) {
      fprintf(stderr, "coalesce: could not start worker '%s'\n", name);
      return 1;
    }
  }

  char st[64];
  if (exchange(fd, MSG_TRIGGER, st, sizeof st) < 0) {
    fprintf(stderr, "coalesce: trigger failed\n");
    return 1;
  }
  return 0;
}

static int usage(void) {
  fprintf(stderr,
          "usage:\n"
          "  coalesce run NAME -- COMMAND [ARG...]\n"
          "  coalesce trigger NAME\n"
          "  coalesce poke NAME -- COMMAND [ARG...]\n"
          "  coalesce status NAME\n"
          "  coalesce cancel NAME\n");
  return 2;
}

int main(int argc, char **argv) {
  if (argc < 2) return usage();
  const char *cmd = argv[1];

  if (!strcmp(cmd, "run") || !strcmp(cmd, "poke")) {
    if (argc < 5) return usage();
    const char *name = argv[2];
    if (check_name(name)) return 2;
    if (strcmp(argv[3], "--") != 0) return usage();
    char **cargv = &argv[4];
    if (!cargv[0]) return usage();
    if (!strcmp(cmd, "run")) {
      char path[PATH_MAX];
      if (socket_path(name, path, sizeof path) < 0) die("socket_path");
      return run_worker(name, path, cargv);
    }
    return cmd_poke(name, cargv);
  }

  if (!strcmp(cmd, "trigger") || !strcmp(cmd, "cancel") ||
      !strcmp(cmd, "status")) {
    if (argc != 3) return usage();
    const char *name = argv[2];
    if (check_name(name)) return 2;
    if (!strcmp(cmd, "trigger")) return client_cmd(name, MSG_TRIGGER, 0);
    if (!strcmp(cmd, "cancel")) return client_cmd(name, MSG_CANCEL, 0);
    return client_cmd(name, MSG_STATUS, 1);
  }

  return usage();
}
