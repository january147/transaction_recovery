#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Date: Thu Nov 14 19:55:33 2019
# Author: January

import json
import sqlite3 as sl
import mmap
import os

class LogEntry():
    def __init__(self, lsn, _type, trans_id, prelsn = -1, seg_offset = -1, undo_nxt_lsn = -1, redo_info = None, undo_info = None):
        self.lsn = lsn
        self.type = _type
        self.trans_id = trans_id
        self.prelsn = prelsn
        self.seg_offset = seg_offset
        self.undo_nxt_lsn = undo_nxt_lsn
        self.redo_info = redo_info
        self.undo_info = undo_info

class LogManager():
    
    def __init__(self, log_filename="log"):
        self.log_file = log_filename
        self.log_db =  sl.connect(log_filename)
        self.next_lsn = -1 

    def create_log_table():
        sql = '''CREATE TABLE log
        (
            LSN INT PRIMARY KEY NOT NULL,
            TYPE CHAR(10) 
            TRANS_ID INT
            PRELSN INT
            SEG_OFFSET INT
            UNDO_NXT_LSN INT
            REDO_INFO TEXT
            UNDO_INFO TEXT
        )
        '''
    def log(self, log_entry):
        lsn = self.get_lsn()
        log_entry.lsn = lsn

        return lsn
    
    def generate_insert_sql(self, log_entry):
        sql = ""
        return sql

    def truncate_log(self):
        pass

    def check_point():
        pass

    def get_lsn(self):
        if self.next_lsn > 0 :
            lsn = self.next_lsn
            self.next_lsn += 1
        else:
            try:
                self.log_file_handle = open(self.log_file, 'r')
                lines = self.log_file_handle.readlines()
                self.log_file_handle.close()
                last_lsn = int(lines[-1].split(' ')[0])
                lsn = last_lsn + 1
            except:
                lsn = 1
            self.next_lsn = lsn + 1
        return lsn


# Disk format
# struct disk{
#     uint32 free_space;
#     uint8[4*1024*1024] data;  
# }
#
# format_segment(ARIES PAGE)
# struct page{
#     uint32 lsn;
#     uint8[4*1024 - 4] data;  
# }
#
class MmapWrapper():
    def __init__(self, mob):
        self.mob = mob
    
    def write(self, offset:int, data:bytes):
        self.mob.seek(offset)
        return self.mob.write(data)
    
    def read(self, offset:int, size:int):
        self.mob.seek(offset)
        return self.mob.read(size)
    
    def flush(self):
        self.mob.flush

class DirtySegmentTable():
    def __init__(self):
        self.data = []

    def insert(self, entry):
        self.data.append(entry)

class BufferManager():

    def __init__(self, disk_image = "disk"):
        self.segment_file = disk_image
        self.buffer = MmapWrapper(self.map_disk(disk_image))
        self.used = self.get_used()
    
    def get_used(self):
        used = int.from_bytes(self.f.read(4), 'little')
        return used
    
    def map_disk(self, disk_image):
        self.f = open(disk_image, 'rb+')
        mob = mmap.mmap(self.f.fileno(), 4096*1025, access=os.O_RDWR, offset=4)
        return mob
        
    def dump(self):
        self.buffer.flush()
    
    def format_segment(self, offset:int, lsn:int):
        lsn_bytes = lsn.to_bytes(4, 'little')
        self.buffer.write(offset, lsn_bytes)
    
    def read_lsn(self, offset):
        lsn_bytes = self.buffer.read(offset, 4)
        lsn = int.from_bytes(lsn_bytes, 'little')
        return lsn

    def write_lsn_segment(self, lsn:int, offset:int, data:bytes):
        lsn_bytes = lsn.to_bytes(4, 'little')
        full_data = lsn_bytes + data
        self.buffer.write(offset, full_data)
    
    def write_segment(self, offset, data):
        self.buffer.write(offset, data)

class TransactionTable():
    def __init__(self):
        self.data = []
    
    def insert(self, entry):
        self.data.append(entry)

class TransactionManager():
    def __init__(self):
        pass



def main():
    log = LogManager()
    buf = BufferManager()
    buf.load()

    transaction_id = [1, 2, 3]
    segement_id = [[1,2,4], [3,5,4], [2,4,7]]
    data = ['v1', 'v2', 'v3']

    for t, s, d in zip(transaction_id, segement_id, data):
        lsns = []
        for s_id in s:
            lsns.append(log.log(t, s_id, d))
        
        for s_id, lsn in zip(s, lsns):
            buf.write(s_id, lsn, d)
    
    choice = input("abort?")
    if choice == 'y':
        exit(0)
    buf.dump()



if __name__ == "__main__":
    main()
