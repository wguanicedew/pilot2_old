import inspect
import commands
import os
import re
import signal
import sys
import time
import Queue
import multiprocessing
import subprocess
import threading
import json

try:
    import yampl
except:
    print "Failed to import yampl: %s" % traceback.format_exc()


class EventService():
    class MessageThread(threading.Thread):
        def __init__(self, messageQ, socketname, context, **kwds):
            threading.Thread.__init__(self, **kwds)
            self.__messageQ = messageQ
            self._stop = threading.Event()
            try:
                self.__messageSrv = yampl.ServerSocket(socketname, context)
            except:
                raise PilotException("Failed to start yampl server socket.")

        def send(self, message):
            try:
                self.__messageSrv.send_raw(message)
            except:
                raise PilotException("Failed to send yampl message.")

        def stop(self):
            self._stop.set()

        def stopped(self):
            return self._stop.isSet()

        def run(self):
            try:
                while True:
                    if self.stopped():
                        break
                    size, buf = self.__messageSrv.try_recv_raw()
                    if size == -1:
                        time.sleep(0.00001)
                    else:
                        self.__messageQ.put(buf)
            except:
                raise PilotException("Message thread failure.")


    def __init__(self, in_queue, out_queue):
        self.__message_queue_to_payload = in_queue
        self.__message_queue_from_payload = out_queue

    def preSetup(self):
        pass

    def postRun(self):
        pass

    def init_message_thread(self, socketname='EventService_EventRanges', context='local'):
        try:
            self.__messageThread = EventServiceManager.MessageThread(self.__message_queue_from_payload, socketname, context)
            self.__messageThread.start()
        except:
            self.terminate()

    def init_payload_process(self, cmd):
        pass

    def init(self, socketname='EventService_EventRanges', context='local', athenaMPCmd=None):
        self.__childRetStatus = 0
        child_pid = os.fork()
        if child_pid == 0:
            # child process
            self.init_message_thread(socketname=socketname, context=context)
            self.init_payload_process(athenaMPCmd)
            while True:
                if self.is_payload_dead():
                   self.terminate()
                   break
                try:
                    message = self.__messageInQueue.get(False)
                    self.__messageThread.send(message)
                except Queue.Empty:
                    pass
                except:
                    pass

            self.terminateChild()
            # sys.exit(0)
            os._exit(0)
        else:
            self.monitor(child_pid)
            return 0
            
    def insertEventRange(self, message):
        self.__log.debug("Rank %s: insertEventRange to ESJobManager: %s" % (self.__rank, message))
        self.__eventRanges.append(message)
        self.__athenaMP_needEvents -= 1
        self.__insertedMessages += 1
        if not "No more events" in message:
            eventRangeID = message['eventRangeID']
            if not eventRangeID in self.__eventRangesStatus:
                self.__eventRangesStatus[eventRangeID] = {}
                self.__eventRangesStatus[eventRangeID]['status'] = 'new'
            #eventRanges= eval(message)
            #for eventRange in eventRanges:
            #    eventRangeID = eventRange['eventRangeID']
            #    if not eventRangeID in self.__eventRangesStatus:
            #        self.__eventRangesStatus[eventRangeID] = {}
            #        self.__eventRangesStatus[eventRangeID]['status'] = 'new'
        else:
            self.__athenaMP_needEvents = 0
            self.__noMoreEvents = True

    def insertEventRanges(self, messages):
        self.__log.debug("Rank %s: insertEventRanges to ESJobManager: %s" % (self.__rank, messages))
        for message in messages:
            self.__athenaMP_needEvents -= 1
            self.__insertedMessages += 1
            self.__eventRanges.append(message)
            if not "No more events" in message:
                eventRangeID = message['eventRangeID']
                if not eventRangeID in self.__eventRangesStatus:
                    self.__eventRangesStatus[eventRangeID] = {}
                    self.__eventRangesStatus[eventRangeID]['status'] = 'new'
            else:
                self.__athenaMP_needEvents = 0
                self.__noMoreEvents = True

    def getEventRanges(self):
        if len(self.__eventRanges) > 0:
            eventRanges = self.__eventRanges.pop(0)
            self.__log.debug("Rank %s: getEventRanges from ESJobManager(will send to AthenaMP): %s" % (self.__rank, eventRanges))
            return eventRanges
        return None

    def sendEventRangeToAthenaMP(self, eventRanges):

        if "No more events" in eventRanges:
            self.__messageInQueue.put(eventRanges)
        else:
            if type(eventRanges) is not list:
                eventRanges = [eventRanges]
            eventRangeFormat = json.dumps(eventRanges)
            self.__messageInQueue.put(eventRangeFormat)


    def getOutput(self):
        if len(self.__outputMessage) > 0:
            output = self.__outputMessage.pop(0)
            self.__log.debug("Rank %s: getOutput from ESJobManager(main prog will handle output): %s" % (self.__rank, output))
            return output
        return None

    def getOutputs(self, signal=False):
        outputs = []
        if not signal:
            if len(self.__outputMessage) > 0:
                outputs = self.__outputMessage
                self.__outputMessage = []
                self.__log.debug("Rank %s: getOutputs from ESJobManager(main prog will handle outputs): %s" % (self.__rank, outputs))
                return outputs
        else:
            if len(self.__outputMessage) > 0:
                self.__log.debug("Rank %s: getOutputs signal from ESJobManager(main prog will handle outputs): %s" % (self.__rank, self.__outputMessage))
                return self.__outputMessage
        return None

    def updatedOutputs(self, outputs):
        for output in outputs:
            try:
                self.__outputMessage.remove(output)
            except:
                self.__log.debug("Rank %s: updatedOutputs failed to updated message: %s" % (self.__rank, output))

    def getEventRangesStatus(self):
        return self.__eventRangesStatus

    def isChildDead(self):
        # if self.__TokenExtractorProcess is None or self.__TokenExtractorProcess.poll() is not None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive():
        # if self.__TokenExtractorProcess is None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive(): 
        #     return True
        if (self.__TokenExtractorCmd is not None and self.__TokenExtractorProcess is None) or self.__athenaMPProcess is None:
            self.__log.debug("Rank %s: TokenExtractorProcess: %s, athenaMPProcess: %s" % (self.__rank, self.__TokenExtractorProcess, self.__athenaMPProcess))
            return True
        if self.__athenaMPProcess.poll() is not None:
            self.__log.debug("Rank %s: AthenaMP process dead: %s" % (self.__rank, self.__athenaMPProcess.poll()))
            return True
        if not self.__messageThread.is_alive():
            self.__log.debug("Rank %s: Yampl message thread isAlive: %s" % (self.__rank, self.__messageThread.is_alive()))
            return True
        return False

    def isDead(self):
        if self.__child_pid is None:
            self.__log.debug("Rank %s: Child process id is %s" % (self.__rank, self.__child_pid))
            if self.__endTime is None:
                self.__endTime = time.time()
            if self.__helperThread: self.__helperThread.stop()
            return True
        try:
            pid, status = os.waitpid(self.__child_pid, os.WNOHANG)
        except OSError, e:
            self.__log.debug("Rank %s: Exception when checking child process %s: %s" % (self.__rank, self.__child_pid, e))
            if "No child processes" in str(e):
                self.__childRetStatus = 0
                if self.__endTime is None:
                    self.__endTime = time.time()
                if self.__helperThread: self.__helperThread.stop()
                return True
        else:
            if pid: # finished
                self.__log.debug("Rank %s: Child process %s finished with status: %s" % (self.__rank, pid, status%255))
                self.__childRetStatus = status%255
                if self.__endTime is None:
                    self.__endTime = time.time()
                if self.__helperThread: self.__helperThread.stop()
                return True
        return False

    def getChildRetStatus(self):
        return self.__childRetStatus

    def isReady(self):
        #return self.__athenaMP_isReady and self.__athenaMPProcess.poll() is None
        #return self.__athenaMP_needEvents > 0 and self.__athenaMPProcess.poll() is None
        return len(self.__eventRanges) > 0 and (not self.isDead()) and self.__athenaMP_isReady

    def isNeedMoreEvents(self):
        #return self.__athenaMP_isReady and len(self.__eventRanges) == 0
        #return self.__athenaMP_needEvents
        if self.__noMoreEvents:
            return 0
        neededEvents = int(self.__numOutputs) + int(self.__ATHENA_PROC_NUMBER) - int(self.__insertedMessages)
        if neededEvents > 0:
            return neededEvents
        return self.__athenaMP_needEvents

    def extractErrorMessage(self, msg):
        """ Extract the error message from the AthenaMP message """

        # msg = 'ERR_ATHENAMP_PROCESS 130-2068634812-21368-1-4: Failed to process event range'
        # -> error_acronym = 'ERR_ATHENAMP_PROCESS'
        #    event_range_id = '130-2068634812-21368-1-4'
        #    error_diagnostics = 'Failed to process event range')
        #
        # msg = ERR_ATHENAMP_PARSE "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', u'startEvent': 5, u'GUID': u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'": Wrong format
        # -> error_acronym = 'ERR_ATHENAMP_PARSE'
        #    event_range = "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', ..
        #    error_diagnostics = 'Wrong format'
        #    -> event_range_id = '130-2068634812-21368-1-4' (if possible to extract)

        error_acronym = ""
        event_range_id = ""
        error_diagnostics = ""

        # Special error acronym
        if "ERR_ATHENAMP_PARSE" in msg:
            # Note: the event range will be in the msg and not the event range id only 
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ (.+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range = found[0][1] # Note: not the event range id only, but the full event range
                    error_diagnostics = found[0][2]
                except Exception, e:
                    self.__log.error("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "EXTRACTION_FAILURE"
                    error_diagnostics = e
                else:
                    # Can the event range id be extracted?
                    if "eventRangeID" in event_range:
                        pattern = re.compile(r"eventRangeID\'\:\ ?.?\'([0-9\-]+)")
                        found = re.findall(pattern, event_range)
                        if len(found) > 0:
                            try:
                                event_range_id = found[0]
                            except Exception, e:
                                self.__log.error("!!WARNING!!2212!! Failed to extract event_range_id: %s" % (e))
                            else:
                                self.__log.error("Extracted event_range_id: %s" % (event_range_id))
                    else:
                        self.__log.error("!!WARNING!!2213!1 event_range_id not found in event_range: %s" % (event_range))
        else:
            # General error acronym
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ ([0-9\-]+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range_id = found[0][1]
                    error_diagnostics = found[0][2]
                except Exception, e:
                    self.__log.error("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "ERR_EXTRACTION_FAILURE"
                    error_diagnostics = e
            else:
                self.__log.error("!!WARNING!!2212!! Failed to extract AthenaMP message")
                error_acronym = "ERR_EXTRACTION_FAILURE"
                error_diagnostics = msg

        return error_acronym, event_range_id, error_diagnostics


    def get_event_ranges(self):
        if not self.get_event_ranges_hook:
            raise NotImplemented("Hook get_event_ranges_hook is not set.")
        return self.get_event_ranges_hook()

    def send_event_ranges_to_payload(self, ranges):
        if "No more events" in ranges:
            self.__messageInQueue.put(ranges)
        else:
            if type(ranges) is not list:
                ranges = [ranges]
            rangesFormat = json.dumps(ranges)
            self.__messageInQueue.put(rangesFormat)

    def process_ready_events(self):
        ranges = self.get_event_ranges()
        self.send_event_ranges_to_payload(ranges)

    def handleMessage(self):
        try:
            message = self.__messageQueue.get(False)
        except Queue.Empty:
            return False
        else:
            if "Ready for events" in message:
                self.process_ready_events()
            elif message.startswith("/"):
                self.__totalProcessedEvents += 1
                self.__numOutputs += 1
                # self.__outputMessage.append(message)
                try:
                    # eventRangeID = message.split(',')[0].split('.')[-1]
                    eventRangeID = message.split(',')[-3].replace("ID:", "").replace("ID: ", "")
                    self.__eventRangesStatus[eventRangeID]['status'] = 'finished'
                    self.__eventRangesStatus[eventRangeID]['output'] = message
                    self.__outputMessage.append((eventRangeID, 'finished', message))
                except Exception, e:
                    self.__log.warning("Rank %s: output message format is not recognized: %s " % (self.__rank, message))
                    self.__log.warning("Rank %s: %s" % (self.__rank, str(e)))
            elif message.startswith('ERR'):
                self.__log.error("Rank %s: Received an error message: %s" % (self.__rank, message))
                error_acronym, eventRangeID, error_diagnostics = self.extractErrorMessage(message)
                if eventRangeID != "":
                    try:
                        self.__log.error("Rank %s: !!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (self.__rank, error_acronym, error_diagnostics, eventRangeID))
                        self.__eventRangesStatus[eventRangeID]['status'] = 'failed'
                        self.__eventRangesStatus[eventRangeID]['output'] = message
                        self.__outputMessage.append((eventRangeID, error_acronym, message))
                    except Exception, e:
                        self.__log.warning("Rank %s: output message format is not recognized: %s " % (self.__rank, message))
                        self.__log.warning("Rank %s: %s" % (self.__rank, str(e)))
                if "FATAL" in error_acronym:
                    self.__log.error("Rank %s: !!WARNING!!2146!! A FATAL error was encountered, prepare to finish" % (self.__rank))
                    self.terminate()
            else:
                self.__log.error("Rank %s: Received an unknown message: %s" % (self.__rank, message))
            unblock_sig(signal.SIGTERM)
            return True







    def poll(self):
        try:
            if self.isDead():
                self.__log.warning("Rank %s: One Process in ESJobManager is dead." % self.__rank)
                self.terminate()
                return -1

            while self.handleMessage():
                pass
            if self.__waitTerminate:
                self.finish()
            else:
                while self.isReady():
                    self.__log.info("Rank %s: AthenMP is ready." % self.__rank)
                    eventRanges = self.getEventRanges()
                    if eventRanges is None:
                        return -1
                    else:
                        self.__log.info("Rank %s: Process Event: %s" % (self.__rank, eventRanges))
                        self.sendEventRangeToAthenaMP(eventRanges)
                        if "No more events" in eventRanges:
                            self.__log.info("Rank %s: ESJobManager is finishing" % self.__rank)
                            self.__log.info("Rank %s: wait AthenaMP to finish" % self.__rank)
                            self.__startTerminateTime = time.time()
                            self.__waitTerminate = True
                            return 0
        except:
            self.__log.warning("Rank %s: Exception happened when polling: %s" % (self.__rank, str(traceback.format_exc())))


    def flushMessages(self):
        block_sig(signal.SIGTERM)

        self.__log.info("Rank %s: ESJobManager flush messages" % self.__rank)
        while self.isReady():
            self.__log.info("Rank %s: AthenaMP is ready, send 'No more events' to it." % self.__rank)
            self.sendEventRangeToAthenaMP("No more events")
        while self.handleMessage():
            pass

        unblock_sig(signal.SIGTERM)

    def run(self):
        try:
            self.init()
        except:
            pass
