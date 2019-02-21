#!/usr/bin/env python

import sys
import os
import subprocess
from multiprocessing.dummy import Pool as ThreadPool
import logging

def usage():
    print()
    print("Python Multithreaded Rsync Handler.")
    print("Usage: rsync_threading.py [Source Path] [Destination Path] [Thread Count]")
    print()
    print()


    sys.exit(0)

def create_target_folder_structure(trim_path):
    #Creates mirror subfolder directory structure if it doesn't already exist
    if not os.path.exists(target_path+trim_path):
        try:
            os.makedirs(target_path+trim_path)
        except:
            logger.error("Error encountered creating / verifying path %s" % trim_path)


def rsync_command(trim_path):
    #Calls rsync with filter flags to only touch files, not subfolders. 
    #command = subprocess.call(["echo", source_path+trim_path, target_path+trim_path])
    command = subprocess.call(["rsync", "-a" ,'-f', "- */", '-f', "+ *", "--ignore-times", "--omit-dir-times", "--no-perms", "--delete", source_path+trim_path, target_path+trim_path])
    try:
        command
    except:
        logger.error("Rsync encountered an error with path %s" % trim_path)
    

    
def work_assign_loop(source_path):
    #generates lists of work to hand out
    for dirpath, dirnames, filenames in os.walk(source_path):
        #if a folder has no subfolders but has files, write it to our work list
        if not dirnames:
            #if the path doesn't end with a /, append one to make life easier later
            if not dirpath.rfind("/") == len(dirpath) -1:
                dirpath = dirpath + "/"
                work_list_base.append(dirpath)
            else:
                work_list_base.append(dirpath)
        #If a folder has subfolders but HAS files as well.
        elif dirnames and len(filenames):
            if not dirpath.rfind("/") == len(dirpath) -1:
                dirpath = dirpath + "/"
                work_list_base.append(dirpath)
            else:
                work_list_base.append(dirpath)
    #trim out the source path so we can create any missing folders before rsyncing files only. Makes Rsync quicker, and avoids threads stepping on each other 
    for item in work_list_base:
        work_list_trim.append(item.replace(source_path,"")) 


def rsync_threading(thread_count):
    #Rsync multithreading. Hope it's faster lol
    logger.info('Starting Rsync batch jobs.')
    pool = ThreadPool(thread_count) 
    pool.map(rsync_command, work_list_trim)
    pool.close() 
    pool.join()
    logger.info('Finished Rsync batch jobs.')


def create_folder_structure_threading(thread_count):
    #Pre-create folder structure multithreading
    logger.info('Starting directory structure preload/verification.')
    pool = ThreadPool(thread_count) 
    pool.map(create_target_folder_structure, work_list_trim)
    pool.close() 
    pool.join()
    logger.info('Finished directory structure preload/verification.')


def main():
    
    global source_path
    global target_path
    global max_threads
    global work_list_base
    global work_list_trim
    global logger 
    
    if len(sys.argv[1:]) != 3 :
        usage()
    
    else:
        source_path = sys.argv[1]
        target_path = sys.argv[2]
        max_threads = int(sys.argv[3])

        work_list_base = []
        work_list_trim = []
        
        logger = logging.getLogger('rsync_threading_log')
        handler = logging.FileHandler('/var/log/replication/rsync_threading.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    if not os.path.exists(source_path):
        print("Could not find source path, please check script arguements")
        usage()

    if not os.path.exists(target_path):
        print("Could not find target path, please check script arguements")
        usage()

              
    work_assign_loop(source_path)   
    create_folder_structure_threading(max_threads)    
    rsync_threading(max_threads)
    
    

main()
