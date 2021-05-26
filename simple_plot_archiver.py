#### This is still under development don't use this code!
#### Plots are being partially copied and it may ruin your hard work.

import argparse
import hashlib
import logging
import os
import shutil
import time

from pathlib import Path
from queue import Queue
from threading import Thread, Lock

log = logging.getLogger(__name__)
log.setLevel(logging.NOTSET)
logging.basicConfig(filename="./simple_plot_archiver_logs.log", level=logging.NOTSET)

K = 32
WORKER_DAEMON_THREADS = 1
RUN_WORKER_DAEMON = True

thread_dest_dir_lst = []
thread_plot_lst = []

lock = Lock()


def space_available(dest_dir):
    """Calculate the free space on the drive after a plot with the given k size is added"""
    plot_size = ((2 * K) + 1) * (2 ** (K - 1)) * 0.762
    free_space = shutil.disk_usage(dest_dir).free
    free_space = free_space - plot_size

    return free_space >= 0


def worker_daemon(source_dir, conf, thread_num):
    """Move plots from their source buffer directory into their final storage location"""
    dest_dirs = []
    d_idx = 0
    plot_src = Path(source_dir)

    while RUN_WORKER_DAEMON:
        # open the conf file and read its contents if there are new files
        # append them to the list destination dirs
        # reading this file at the start of each iteration allows it to be
        # modified on the fly without restarting the archiver.
        with open(conf, "r") as fp:
            while newline := fp.readline():
                dest_path = Path(newline.rstrip("\n"))

                if dest_path.exists() is False or dest_path.is_dir() is False:
                    log.error(f"Invalid destination directory: {dest_path}")
                else:
                    abs_path = str(dest_path.absolute())
                    if len(list(filter(lambda x: x == abs_path, dest_dirs))) == 0:
                        dest_dirs.append(abs_path)

        #######################
        # Begin unsafe code
        #######################
        lock.acquire()

        idx_chosen = False
        drives_full = True
        # Get the next available index with free space
        i = 0
        while i < len(dest_dirs) and drives_full is True:
            d_idx += 1
            if d_idx >= len(dest_dirs):
                d_idx = 0
            if space_available(dest_dirs[d_idx]):
                drives_full = False
                idx_chosen = True

        if drives_full:
            log.warning(f"No drives with enough free space found! Thread: {thread_num}")
            time.sleep(1)
            lock.release()
            continue

        # Chosen index is already being used, find an index to a destination directory
        # without a file going on
        if dest_dirs[d_idx] in thread_dest_dir_lst:
            idx_chosen = True
            matching_idxs = []
            for thr_d_dir in thread_dest_dir_lst:
                for d_idx, d_dir in enumerate(dest_dirs):
                    if thr_d_dir == d_dir:
                        # Get a list of indexes that already are in use and
                        # already have a file being transferred to them
                        matching_idxs.append(d_idx)

            dest_dir_idxs = list(range(0, len(dest_dirs)))
            # Get a list of the indexes of destination directories that are not
            # in the list of directories that are already in use
            valid_idxs = list(set(dest_dir_idxs) - set(matching_idxs))

            for v_idx in valid_idxs:
                free_space = space_available(dest_dirs[v_idx])
                if free_space:
                    idx_chosen = True
                    d_idx = v_idx
                    break

        if idx_chosen is False:
            # We dont need to stop the process just let the user know
            log.warning(f"No drives with enough free space found! Thread: {thread_num}")
            time.sleep(1)
            lock.release()
            continue

        thread_dest_dir_lst[thread_num] = dest_dirs[d_idx]

        # Look in the source directory for plots of the correct k value and
        # without .tmp ending and with the .plot ending
        plot_found = False
        plot_lst = [x for x in plot_src.iterdir()]
        plot_lst = list(filter(lambda x: "plot" in x.name, plot_lst))
        if len(plot_lst) > 0:
            for plot_path in plot_lst:
                basename = str(plot_path.name).lower()
                abs_path = str(plot_path.absolute())
                if "tmp" not in basename and basename.endswith(".plot"):
                    # Check that plot is of the correct k type
                    k_val = None
                    for k in ["k32", "k33", "k34", "k35"]:
                        if k in basename[:10]:
                            k_val = int(k[1:])

                    # check plot size
                    plt_size_b = plot_path.stat().st_size
                    # Out of 107 plots 108758287484 was the min size 108943399155 the max
                    if plt_size_b < 108700000000:
                        log.error(f"Plot is of incorrect size: {abs_path}")
                    elif not k_val:
                        log.error(f"Unable to discern plot k value: {abs_path}")
                    elif abs_path not in thread_plot_lst and k_val == K:
                        plot_found = True
                        thread_plot_lst[thread_num] = abs_path
                        break

        #######################
        # Done with unsafe code
        #######################
        lock.release()

        if plot_found is False:
            # Sleep for a bit in case there are no files to transfer
            log.warning(f"No plots found in {str(plot_src)}")
            time.sleep(1)
            continue

        src = Path(thread_plot_lst[thread_num])
        dst = Path(thread_dest_dir_lst[thread_num])

        # Something is wrong but we don't necessicarily need to exit, just alert
        # the user so they can take care of it.
        if src.exists() is False:
            log.error(f"Source plot {src} does not exist")
            continue
        if dst.exists() is False and dst.is_dir() is False:
            log.error(f"Destination directory invalid: {dst}")
            continue

        log.info(f"Moving {str(src.name)} to {str(dst)}")
        print(f"Moving {str(src.name)} to {str(dst)}")

        copy_complete = False
        i = 0
        while (copy_complete is False or i < 5):
#            os.system(f"cp {thread_plot_lst[thread_num]} {thread_dest_dir_lst[thread_num]}") # Not OS agnostic
             time.sleep(15) # Sleep for 15 seconds just to make sure any io is done on the file
             shutil.copy2(src, dst, follow_symlinks=False)

             src_md5 = hashlib.md5(str(src).encode()).hexdigest()
             dst_md5 = hashlib.md5(str(dst).encode()).hexdigest()
             if (src_md5 == dst_md5):
                 copy_complete = True
                 src_md5.unlink(missing_ok=True) # Safe to delete the old plot
             else:
                 log.info(f"Unable to move {str(src.name)} to {str(dst)} SOMETHING IS VERY WRONG!")
                 dst_md5.unlink(missing_ok=True) # Delete and try again
             i += 1


        log.info(f"Moved {str(src.name)} to {str(dst)}")

        thread_dest_dir_lst[thread_num] = ""
        thread_plot_lst[thread_num] = ""


def main():
    parser = argparse.ArgumentParser(description="Start Chia archiver")

    parser.add_argument(
        "-c",
        "--conf",
        type=str,
        default=None,
        help="A text file that has final plot destinations seperated by newlines\n"
        "\tPlots will be distributed in round robin fashion",
    )
    parser.add_argument(
        "-d",
        "--dir",
        type=str,
        default=None,
        help="The directory where to retreive plots from",
    )

    args = parser.parse_args()

    if args.dir is None:
        log.error("Plot source directory not specified!")
        return
    if args.conf is None:
        log.error("Destination directory configuration file not specified!")
        return

    threads = []
    for i in range(0, WORKER_DAEMON_THREADS):
        thread_dest_dir_lst.append("")
        thread_plot_lst.append("")
        t = Thread(
            target=worker_daemon,
            kwargs={"source_dir": args.dir, "conf": args.conf, "thread_num": i},
        )
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join(timeout=None)


if __name__ == "__main__":
    main()
