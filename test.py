#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Date: Thu Nov 14 19:55:33 2019
# Author: January

import json
import sqlite3 as sl
import mmap
import os
import math
import traceback
import pdb
import base64

class LogEntry():
    UPDATE = 'U'
    END_CK = 'ECK'
    BEGIN_CK = 'BCK'
    END_TC = 'ETC'
    CLR = 'CLR'
    def __init__(self,_type, trans_id = -1,  lsn = -1, prelsn = -1, seg_offset = -1, undo_nxt_lsn = -1, redo_info = b'', undo_info = b''):
        self.lsn = lsn
        self.type = _type
        self.trans_id = trans_id
        self.prelsn = prelsn
        self.seg_offset = seg_offset
        self.undo_nxt_lsn = undo_nxt_lsn
        self.redo_info = redo_info
        self.undo_info = redo_info

    @staticmethod
    def parse(log_entry):
        lsn, _type, trans_id, prelsn, seg_offset, undo_nxt_lsn, redo_info, undo_info = log_rec
        redo_info = base64.b64decode(redo_info)
        undo_info = base64.b64decode(undo_info)
        log_entry_ob = LogEntry(lsn, _type, trans_id, prelsn, seg_offset, undo_nxt_lsn, redo_info, undo_info)
        return log_entry_ob

class LogManager():
    
    def __init__(self, log_filename="data.log"):
        self.log_file = log_filename
        self.log_db = sl.connect(log_filename)
        self.next_lsn = -1 

    # 1.LSN,
    # 2.TYPE,
    # 3.TRANS_ID,
    # 4.PRELSN,
    # 5.SEG_OFFSET,
    # 6.UNDO_NXT_LSN,
    # 7.REDO_INFO,
    # 8.UNDO_INFO
    def create_log_table(self):
        sql = '''CREATE TABLE log(
            LSN INT NOT NULL,
            TYPE CHAR(10) NOT NULL,
            TRANS_ID INT NOT NULL,
            PRELSN INT,
            SEG_OFFSET INT,
            UNDO_NXT_LSN INT,
            REDO_INFO TEXT,
            UNDO_INFO TEXT,
            PRIMARY KEY(LSN)
        );
        '''
        self.log_db.execute(sql)
        self.log_db.commit()
    def create_master_rec(self):
        sql = '''CREATE TABLE master_rec(
            LSN INT,
            PRIMARY KEY(LSN)
        );
        '''
        self.log_db.execute(sql)
        self.log_db.commit()

    def log(self, log_entry):
        lsn = self.get_lsn()
        log_entry.lsn = lsn
        sql = self.generate_insert_sql(log_entry)
        self.log_db.execute(sql)
        self.log_db.commit()
        return lsn

    def get_master_rec(self):
        sql = "SELECT * FROM master_rec ORDER BY LSN DESC LIMIT 1"
        cursor = self.log_db.execute(sql)
        master_rec = cursor.fetchone()
        if master_rec is None:
            master_rec_lsn = 0
        else:
            master_rec_lsn = master_rec[0]
        return master_rec_lsn
    
    def log_master_rec(self, lsn:int):
        sql = "SELECT * FROM master_rec ORDER BY LSN DESC LIMIT 1"
        cursor = self.log_db.execute(sql)
        master_rec = cursor.fetchone()
        if master_rec is None:
            sql = "INSERT INTO master_rec VALUES (%d)"%(lsn)
        else:
            sql = "UPDATE master_rec SET LSN = %d"%(lsn)
        cursor.execute(sql)
        self.log_db.commit()
    
    def generate_insert_sql(self, log_entry):
        log_entry.redo_info = base64.b64encode(log_entry.redo_info).decode()
        log_entry.undo_info = base64.b64encode(log_entry.undo_info).decode()
        sql = "INSERT INTO log VALUES (%d, '%s', %d, %d, %d, %d, '%s', '%s');"%(log_entry.lsn, log_entry.type, log_entry.trans_id, log_entry.prelsn, log_entry.seg_offset, log_entry.undo_nxt_lsn, log_entry.redo_info, log_entry.undo_info)
        return sql

    def truncate_log(self):
        pass

    def get_lsn(self):
        if self.next_lsn > 0 :
            lsn = self.next_lsn
            self.next_lsn += 1
        else:
            sql = "SELECT LSN FROM log ORDER BY LSN DESC LIMIT 1"
            cursor = self.log_db.execute(sql)
            last_log = cursor.fetchone()
            if last_log is not None:
                lsn = last_log[0] + 1
            else:
                lsn = 1
            self.next_lsn = lsn + 1
        return lsn
    
    def read_from_lsn(self, lsn):
        sql = "SELECT * FROM log WHERE LSN >= %d;"%(lsn)
        cursor = self.log_db.execute(sql)
        return cursor
    
    def read_by_lsn(self, lsn):
        sql = "SELECT * FROM log WHERE LSN = %d;"%(lsn)
        cursor = self.log_db.execute(sql)
        return cursor.fetchone()

    def close(self):
        self.log_db.commit()
        self.log_db.close()


# Disk format
# struct disk{
#     uint8[1024] disk_info;
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
    
    def close(self):
        self.mob.close()

class DirtySegmentTable():
    def __init__(self):
        self.data = dict()

    def insert(self, seg_offset, rec_lsn):
        self.data[seg_offset] = rec_lsn

    def delete(self, seg_offset):
        self.data.pop(seg_offset)
    
    def clear(self):
        self.data.clear()
        
class BufferManager():
    
    def __init__(self, disk_image = "disk.img"):
        self.segment_file = disk_image
        self.buffer = MmapWrapper(self.map_disk(disk_image))
        self.used = self.get_used()
        self.dst = DirtySegmentTable()
        self.fake_buffer = []
    
    def get_used(self):
        used = int.from_bytes(self.f.read(4), 'little')
        return used
    
    def map_disk(self, disk_image):
        self.f = open(disk_image, 'rb+')
        mob = mmap.mmap(self.f.fileno(), 4096*1024, access=os.O_RDWR)
        return mob
        
    def dump(self):
        for offset, data in self.fake_buffer:
            self.write(offset, data)
        self.buffer.flush()
        self.dst.clear()
    
    def format_segment(self, offset:int, lsn:int):
        lsn_bytes = lsn.to_bytes(4, 'little')
        self.buffer.write(offset, lsn_bytes)
    
    def read_lsn(self, offset):
        lsn_bytes = self.buffer.read(offset, 4)
        lsn = int.from_bytes(lsn_bytes, 'little')
        return lsn

    def read(self, offset, len):
        return self.buffer.read(offset, len)

    def write_lsn_segment(self, lsn:int, offset:int, data:bytes):
        lsn_bytes = lsn.to_bytes(4, 'little')
        full_data = lsn_bytes + data
        if offset not in self.dst.data.keys():
            self.dst.insert(offset, lsn)
        self.buffer.write(offset, full_data)
    
    def write(self, offset:int, data):
        self.buffer.write(offset, data)
    
    def write_segment(self, offset:int, data):
        self.fake_buffer.append((offset, data))
    
    def close(self):
        self.f.close()
        self.buffer.close()

class TransactionTable():
    UPDATE = 'U'
    COMMIT = 'C'
    def __init__(self):
        self.data = dict()
    
    def insert(self, trans_id, status, last_lsn, undo_nxt_lsn):
        self.data[trans_id] = [status, last_lsn, undo_nxt_lsn]
    
    def delete(self, trans_id):
        self.data.pop(trans_id)

class TransactionManager():
    def __init__(self):
        self.tct = TransactionTable()
        self.next_tc_id = 0
    
    def get_trans_id(self):
        id = self.next_tc_id
        self.next_tc_id += 1
        return id

    def checkpoint(self):
        begin_entry = LogEntry(LogEntry.BEGIN_CK)
        # ask log manager to log begin checkpoint
        new_master_rec = logm.log(begin_entry)
        # read transaction table
        tc_table = json.dumps(self.tct.data)
        # read dirty segment table from buffer manager
        ds_table = json.dumps(bufm.dst.data)
        # ask log manager to log end checkpoint
        # use 'redo' to store dirty segment table, use 'undo' to store transaction table
        logm.log(LogEntry.END_CK, redo=ds_table, undo=tc_table)
        # checkpoint ok, record new master_rec
        logm.log_master_rec(new_master_rec) 

    def analysis(self):
        master_rec = logm.get_master_rec_lsn()
        cursor = logm.read_from_lsn(master_rec)
        log_rec = cursor.fetchone()
        if log_rec == None:
            print("No log to recover")
            return
        if log_rec[1] == LogEntry.BEGIN_CK:
            log_rec = cursor.fetchone()
        while log_rec != None:
            lsn, _type, trans_id, prelsn, seg_offset, undo_nxt_lsn, redo_info, undo_info = log_rec 
            trans_id = log_rec[2]
            _type = log_rec[1]
            if trans_id != -1 and trans_id not in self.tct.data.keys():
                # trans_id, status, last_lsn, undo_nxt_lsn
                self.tct.insert(trans_id, TransactionTable.UPDATE, lsn, prelsn)
            if _type == LogEntry.UPDATE or _type == LogEntry.CLR:
                # I don't understand why undo shoud be checked.
                # It's redo that should be checked. Now I understand.
                if len(redo) != 0 and seg_offset not in bufm.dst.data.keys:
                    bufm.dst.insert(seg_offset, lsn)
                # normal update entry
                if _type == LogEntry.UPDATE:
                    pdb.set_trace()
                    if len(undo_info) != 0:
                        self.tct.data[trans_id][2] = lsn
                # clr entry
                else:
                    self.tct.data[trans_id][2] = undo_nxt_lsn
            elif _type == LogEntry.END_CK:
                tct = json.loads(log_rec[7])
                dst = json.loads(log_rec[6])
                for trans_id in tct.keys():
                    if trans_id not in self.tct.data.keys():
                        status, last_lsn, undo_nxt_lsn = tct[trans_id]
                        self.tct.insert(trans_id, status, last_lsn, undo_nxt_lsn)
                for seg_offset in dst.keys():
                    if seg_offset not in bufm.dst.data.keys():
                        dst.insert(seg_offset, dst[seg_offset])
                    else:
                        bufm.dst.data[seg_offset] = dst[seg_offset]
            elif _type == LogEntry.END_TC:
                self.tct.delete(trans_id)
            else:
                pass
            log_rec = cursor.fetchone()
        
        tct = self.tct.data
        for trans_id in tct.keys():
            if tct[trans_id][0] == TransactionTable.UPDATE and tct[trans_id][2] == -1:
                log_entry = LogEntry(LogEntry.END_TC, trans_id)
                logm.log(log_entry)
                self.tct.delete(trans_id)
        redo_lsn = math.inf
        for seg_offset in bufm.dst.data.keys():
            rec_lsn = bufm.dst.data[seg_offset]
            if rec_lsn < redo_lsn:
                redo_lsn = rec_lsn
        return redo_lsn 
        

    def redo_from_lsn(self, lsn:int):
        cursor = logm.read_from_lsn(lsn)
        log_rec = cursor.fetchone()
        while log_rec is not None:
            lsn, _type, trans_id, prelsn, seg_offset, undo_nxt_lsn, redo_info, undo_info = log_rec 
            if _type == LogEntry.UPDATE or _type == LogEntry.CLR:
                if len(redo_info) != 0:
                    bufm.write_segment(seg_offset, redo_info)
            log_rec = cursor.fetchone()
            


    def total_undo(self):
        for trans_id in self.tct.data.keys():
            status, last_lsn, undo_nxt_lsn = self.tct.data[trans_id]
            while tc_undo_nxt_lsn != -1:
                log_rec = logm.read_by_lsn(tc_undo_nxt_lsn)
                lsn, _type, trans_id, prelsn, seg_offset, undo_nxt_lsn, redo_info, undo_info = log_rec 

                if _type == LogEntry.CLR:
                    tc_undo_nxt_lsn = undo_nxt_lsn 
                elif _type == LogEntry.UPDATE and len(undo) != 0:
                    bufm.write_segment(seg_offset, undo_info)
                    # record a CLR when undo a log entry
                    log_entry = LogEntry(LogEntry.CLR, trans_id, prelsn=last_lsn, redo_info=undo_info, undo_nxt_lsn=prelsn)
                    new_lsn = logm.log(log_entry)
                    tc_undo_nxt_lsn = prelsn
                    self.tct.data[trans_id][1] = new_lsn
                self.tct.data[trans_id][2] = tc_undo_nxt_lsn
            # record an end when transaction rollback finished
            log_entry = LogEntry(LogEntry.END_TC, trans_id, prelsn=new_lsn)
            logm.log(log_entry)
            self.tct.delete(trans_id)


    def doUpdateTranscation(self, segment_list, data_list):
        if len(segment_list) != len(data_list):
            print("transaction data error")
            return False
        trans_id = self.get_trans_id()
        prelsn = -1
        self.tct.insert(trans_id, TransactionTable.UPDATE, -1, -1)
        for seg_offset, data in zip(segment_list, data_list):
            size = len(data)
            preimage = bufm.read(seg_offset, size)
            # log
            data = bytes(data, encoding='utf8')
            log_entry = LogEntry(LogEntry.UPDATE, trans_id, seg_offset=seg_offset, prelsn=prelsn, redo_info=data, undo_info=preimage)
            prelsn = logm.log(log_entry)
            self.tct.data[trans_id][1] = prelsn
            # write data
            bufm.write_segment(seg_offset, data)
        # write end entry when transaction finished
        log_entry = LogEntry(LogEntry.END_TC, trans_id, prelsn=prelsn)
        logm.log(log_entry)




# 4*1024个区域

logm = LogManager()
bufm = BufferManager()
tcm = TransactionManager()


def default_tc():
    segment_list = (0x10, 0x20, 0x30)
    data_list = ("init", "do something", "end")
    tcm.doUpdateTranscation(segment_list, data_list)

def main():
    try:
        while True:
            cmd = input(">>> ")
            if cmd == "create":
                log.create_log_table()
            elif cmd == 'do_default_tc':
                default_tc()
            elif cmd == 'q':
                break
            else:
                print("invalid command")
    except Exception as e:
        traceback.print_exc()
    finally:
        logm.close()
        bufm.close()

def test_main():
    default_tc()

if __name__ == "__main__":
    test_main()
