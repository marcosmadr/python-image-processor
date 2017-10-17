#!/usr/bin/python
import shutil
import subprocess
import sys
import re
import os
import time
import random
import string
from PIL import Image
from PIL.ImageDraw import ImageDraw
import datetime
import logging
import threading
import queue
import sqlite3

FRAMES_NUMBER = 49
FRAME_WEIGHT = 680
FRAME_HEIGHT = 472

FRAME_FILE_PREFIX = 'frm-'
PAGE_FILE_PREFIX = 'flipbook-pg-'

WORKDIR = '/tmp/flipbook'

DATABASE_FILE = 'db.sqlite3'
DATABASE_TABLE = 'myflipbook_jobs'

STATE_NEW   = 'NEW'
STATE_PROC  = 'PROCESSING'
STATE_DONE  = 'DONE'

ACTION_FRAME = 'FRAME'
ACTION_PAGE  = 'PAGE'

MAX_THREADS = 2

class PageTemplate(object):
    """Functions to create PNG file to print
    """
    def __init__(self):
        self.pgtype = None

    def create(self, imagelist, pagename):
        if not self.pgtype:
            raise Exception("You have to set a paper type before!")

        if len(imagelist) > self.max_images:
            raise Exception("Too many images for this page [{}]".format(len(imagelist)))

        newImage = Image.new(self.mode, 
                            self.resolution, 
                            self.bgcolor)
        x = self.border
        y = self.border
        pos_x = 1

        for filename in imagelist:
            try:
                img = Image.open(filename)
            except Exception,e:
                logging.error(str(e))
                raise Exception("Error opening image [{}]".format(filename))

            box = (x, y, x+self.frame_weight, y+self.frame_height)
            newImage.paste(img, box)

            if pos_x < self.max_per_x:
                x = x + self.frame_weight + self.border
                pos_x = pos_x + 1
            else:
                x = self.border
                pos_x = 1
                y = y + self.frame_height + self.border
        try:
            newImage.save(pagename)
        except Exception,e:
            logging.error(str(e))
            raise Exception("Cannot save page! [{}]".format(pagename))

        draw = ImageDraw(newImage)
        draw.line((100, 200, 150,300), fill=128)

        logging.info("Page {} done".format(pagename))

    def set_layout(self):
        full_frame_weight = self.frame_weight + self.border
        full_frame_height = self.frame_height + self.border

        self.max_per_x = int(self.weight / full_frame_weight)
        if int( self.weight % full_frame_weight ) < self.border:
            self.max_per_x -= 1

        self.max_per_y = int( self.height / full_frame_height )
        if int( self.height % full_frame_height ) < self.border:
            self.max_per_y -= 1

        self.max_images = self.max_per_x * self.max_per_y


class PageA4(PageTemplate):
    """ Define page properties such as dimensions, border size and background color
    """
    def __init__(self):
        self.pgtype = 'A4'
        self.weight = 4960
        self.height = 3508
        self.mode = "RGB"
        self.bgcolor = "WHITE"
        self.border = 25
        self.frame_weight = FRAME_WEIGHT
        self.frame_height = FRAME_HEIGHT
        self.resolution = (self.weight,self.height)
        self.set_layout()


class PageGenerator(object):
    """ Get all frames and create pages to print
    """
    def __init__(self, workdir, num_frames):
        self.frames = num_frames
        self.dir = workdir
 
    def generate(self):
        pg = PageA4()

        image_counter = 1
        image_list = []
        page = 1
        for frame in range(0, self.frames):
            f = "{}/{}{}.png".format( self.dir, FRAME_FILE_PREFIX, frame )

            if not os.path.exists(f):
                raise Exception("Missing frame [{}]".format(f))

            image_list.append(f)
            if image_counter == pg.max_images or frame == (self.frames-1):
                pn = os.path.join(self.dir, "{}{}.png".format(PAGE_FILE_PREFIX, page))
                pg.create(image_list, pn)
                image_list = []
                image_counter = 1
                page = page + 1
            else:
                image_counter = image_counter + 1


class FfmpegWrapper:
    """ Wrapper to use some ffmpeg functions
    """
    @staticmethod
    def extract_frames(filename, workdir, num_frames, frame_weight, frame_height):
        interval = FfmpegWrapper.get_length(filename) / float(num_frames)
        logging.debug("Rate: {}".format(interval))

        for x in range(0, num_frames):
            command_opt = """ -i {} -s {}x{} -frames:v 1 -an -f image2
                            {}/{}{}.png """.format(filename, frame_weight, \
                            frame_height, workdir, FRAME_FILE_PREFIX, x)

            command = "ffmpeg -accurate_seek -ss {} {}".format((x*interval),\
                      command_opt)

            result = subprocess.Popen(command.split(),
                    stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            result.communicate()

    @staticmethod
    def get_length(filename):
        length_regexp = 'Duration: (\d{2}):(\d{2}):(\d{2})\.\d+,'
        re_length = re.compile(length_regexp)

        result = subprocess.Popen(["ffmpeg", '-i', filename],
        stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

        output = result.stdout.read()
        matches = re_length.search(output)
        if matches:
            video_length = int(matches.group(1)) * 3600 + \
                            int(matches.group(2)) * 60 + \
                            int(matches.group(3))
        else:
            import pprint
            pprint.pprint(output)
            raise Exception("Cannot determine video length!")
        try:
            vl = int(video_length)
        except:
            raise Exception("Invalid video length [{}]".format(video_length))

        return vl


class FlipBook(object):
    """ Flipbook processing manager, according to 'action' it starts a process 
    to generate frames from a movie clip or build a flipbook printable page from
    frames
    STATE_NEW: Task was created on database
    STATE_PROC: Video is on queue or processing
    STATE_DONE: The task was done
    action varialbe can be FRAME or PAGE
    """
    def __init__(self, job_id, input_file, action):
        self.job_id = job_id
        self.workdir = os.path.join(WORKDIR, job_id)
        self.input_file = input_file
        self.action = action
        self._state = STATE_PROC

    def start(self):
        if self.action == ACTION_FRAME:
            self._frame_generator()
        elif self.action == ACTION_PAGE:
            self._page_generator()
        else:
            logging.error("Unknown action? job_id={},{}".format( self.job_id,\
                          self.action))

    def _frame_generator(self):
        self._create_work_dir()
        logging.debug("Processing video... job_id={}".format(self.job_id))
        FfmpegWrapper.extract_frames(self.input_file, self.workdir, FRAMES_NUMBER,
                                     FRAME_WEIGHT, FRAME_HEIGHT)
        self.state = STATE_DONE
        logging.debug("Frame generation finished! job_id={}".format(self.job_id))

    def _page_generator(self):
        logging.debug("Generating pages for printing... job_id={}".format(\
                      self.job_id))
        pgs = PageGenerator(self.workdir, FRAMES_NUMBER)
        pgs.generate()
        self._clean_work_dir()
        self.state = STATE_DONE
        logging.debug("Page generation finished! job_id={}".format(self.job_id))

    def _clean_work_dir(self):
        filelist = [f for f in os.listdir(self.workdir) \
                                             if f.startswith(FRAME_FILE_PREFIX)]
        for f in filelist:
            try:
                os.remove(os.path.join(self.workdir, f))
            except:
                logging.error("Error removing file {}. job_id={}".format(f,\
                              self.job_id))

    def _create_work_dir(self):
        logging.debug("Creating temporary directory job_id={},{}".format(\
                      self.job_id, self.workdir))

        if self.workdir is None:
            raise Exception("Undefined workdir")

        if os.path.exists(self.workdir):
            logging.warning("This path already exists and will be erased. job_id={},{}".format(self.job_id, self.workdir))
            try:
                shutil.rmtree(self.workdir)
            except Exception,e:
                logging.error(str(e))
                raise Exception("Cannot delete directory! {}".format(self.workdir))
        try:
            os.mkdir(self.workdir)
        except Exception,e:
            logging.error(str(e))
            raise Exception("Cannot create directory! {}")

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        db = Db()
        db.execute("""UPDATE `{}` SET path=?, state=? WHERE job_id=?"""\
                  .format(DATABASE_TABLE),(self.workdir, state, self.job_id))
        db.commit()
        db.close()
        self._state = state


class FlipBookWorker(threading.Thread):
    """ The worker thread, handle jobs
    """
    def __init__(self, queue):
        super(FlipBookWorker, self).__init__()
        self._queue = queue
        self.working = False

    def run(self):
        while True:
            job = self._queue.get()
            try:
                self.working = True
                job.start()
            except Exception, e:
                logging.error("Fatal error! job_id={}\n{}".format(job.job_id, e))
            else:
                self._queue.task_done()
            self.working = False


class FlipBookManager:
    def __init__(self):
        self.db = Db()

    def get_new_jobs(self):
        """ Get new jobs from database
        """
        result = self.db.query("""SELECT `job_id`,`video_filename`,`action` FROM {} WHERE state=?"""\
                  .format(DATABASE_TABLE), (STATE_NEW, ))
        for job in result:
            self.db.execute("""UPDATE {} SET state=? WHERE job_id=?""".format(DATABASE_TABLE),
                      (STATE_PROC, job[0]))
        self.db.commit()
        return result


class Db:
    """ Db access layer
    """
    def __init__(self, db=DATABASE_FILE):
        self.conn = None
        self.cursor = None
        self.database = db
        self.connect()

    def connect(self):
        self.conn = sqlite3.connect(self.database)
        self.cursor = self.conn.cursor()

    def execute(self, query, params):
        return self.cursor.execute(query, params)

    def query(self, query, params):
        self.execute(query, params)
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def main():
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
    logging.info("Starting FlipBook Converter...")

    queue_jobs = queue.Queue()

    logging.debug("Starting {} worker threads...".format(MAX_THREADS))
    workers = []
    for worker in range(MAX_THREADS):
        fbw = FlipBookWorker(queue_jobs)
        fbw.daemon = True
        fbw.start()
        workers.append(fbw)

    fbm = FlipBookManager()
    try:
        while True:
            for job in fbm.get_new_jobs():
            #put jobs on queue
               logging.debug("Including job on processing queue. job_id={}".format(job[0])) 
               queue_jobs.put_nowait(FlipBook(*job))
            #write status message
            active_threads = len([ w for w in workers if w.working ])
            logging.debug("{} active jobs, {} on queue, {} total".format(\
                            active_threads, queue_jobs.qsize(),
                            active_threads + queue_jobs.qsize()))

            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Exit.")
        sys.exit(1)

main()
