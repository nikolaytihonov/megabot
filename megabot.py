import os
import sys
import time
import sqlite3
from mega import Mega
from pyrogram import Client

from multiprocessing import Pool

db = sqlite3.connect("megabot.db", check_same_thread=False)
appId, appHash = None, None

mega = Mega()
megaUsr = None

settings = {}

def load_settings():
    kvals = db.cursor().execute("SELECT name,value FROM settings;")
    for (name, value) in kvals:
        settings[name] = value

def get_setting(name):
    value = settings.get(name)
    if not value:
        value = input("Enter %s: " % name)
        db.execute("INSERT INTO settings VALUES (?,?);", (name, value))
        db.commit()
    return value

channels = {}

def load_channels():
    global channels
    
    cur = db.cursor()
    cur.execute("SELECT * FROM mega_channels;")
    for (bId,cId,cTitle,node) in cur.fetchall():
        channels[cId] = (bId,cTitle,node)

def download_media(tg, bId, media):
    download = None
    mediaId = media.file_id
    try: download = tg.download_media(mediaId)
    except Exception as e:
        print("failed to sync media %s: %s" % (mediaId, str(e)))
    
    db.execute("INSERT INTO mega_files VALUES (?,?,?);",
        (mediaId, os.path.split(download)[-1], bId))
    db.commit()
    
    return download

def upload_media(nodeId, download):
    megaUsr.upload(download, nodeId)
    
    os.remove(download)
    return os.path.split(download)[-1]

def sync_channel(tg, cId, bChan):
    bId, cTitle, nodeId = bChan
    
    poolSize = 4
    
    msgCount = tg.get_history_count(cId)
    msgOffset = 0
    
    while msgOffset < msgCount:
        msgs = tg.get_history(cId, limit = poolSize, offset = msgOffset)
        
        medias = []
        
        for msg in msgs:
            try:
                if not msg.media: continue
                
                media = None
                if msg.photo: media = msg.photo
                if msg.video: media = msg.video
                if msg.document: media = msg.document
                
                # 64 MB file limit
                if media.file_size > 64*1024*1024: continue
                
                mediaId = media.file_id
                cur = db.cursor()
                cur.execute("""SELECT * FROM mega_files
                    WHERE file_id=?;""", (mediaId,))
                if len(list(cur.fetchall())) == 0:
                    download = download_media(tg, bId, media)
                    medias.append(download)
            except Exception as e:
                print("message_id %d: %s" % (msg.message_id, str(e)))
        
        with Pool(processes = poolSize) as pool:
            fileNames = pool.starmap(upload_media, [
                (nodeId, m) for m in medias
            ])
            print("\n".join(fileNames))
            
            pool.close()
            pool.join()
        
        msgOffset += len(msgs)

if __name__ == "__main__":
    db.execute(
        """CREATE TABLE IF NOT EXISTS settings 
        (
            name TEXT PRIMARY KEY,
            value TEXT
        );"""
    )

    load_settings()
    print(settings)

    db.execute(
        """CREATE TABLE IF NOT EXISTS mega_channels
        (
            bot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chan_id INTEGER,
            chan_title TEXT,
            mega_node INTEGER
        );"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS mega_files
        (
            file_id TEXT PRIMARY KEY,
            file_name TEXT,
            bot_channel INTEGER
        );"""
    )

    load_channels()
    print(channels)

    megaUsr = mega.login(get_setting("mega_email"),
        get_setting("mega_password"))
    db.commit()
    
    appId = int(get_setting("app_id"))
    appHash = get_setting("app_hash")
    with Client("megabot", appId, appHash) as tg:
        cId, cTitle = None, None
        try:
            chan = tg.get_chat(sys.argv[1])
            try:
                if chan.type != "channel":
                    raise Exception("not a channel")
                cId, cTitle = chan.id, chan.title
            except Exception as e: print("not a chat: %s" % str(e))
        except Exception as e:
            print("failed to get channel: %s" % str(e))
            sys.exit(1)
        
        bChan = None
        if cId in channels:
            bChan = channels.get(cId)
            print(bChan)
        else:
            folderName = "NSFW%d" % cId
            node = megaUsr.create_folder(folderName)
            nodeId = node[folderName]
            bId = len(channels)
            
            bChan = (cId,cTitle,nodeId)
            db.execute(
                """INSERT INTO mega_channels (chan_id,chan_title,mega_node)
                    VALUES (?,?,?);""", bChan)
            db.commit()
        
        sync_channel(tg, cId, bChan)
