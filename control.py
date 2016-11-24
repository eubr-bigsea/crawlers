import time
import datetime

class Control:

    def __init__(self, sleep, control_file):
        ''' Set the init and sleep time, and the name of the file
        that controls the execution frequency'''
        self.init_time = time.time()
        self.sleep_time = float(sleep)
        self.control_file = control_file

    def set_end(self, number_records):
        ''' Set the duration and next execution '''
        self.duration = time.time() - self.init_time
        self.next_execution = time.time() + float(self.sleep_time) - self.duration
        if (self.sleep_time > self.duration):
            self.sleep_time -= self.duration

    def verify_next_execution(self):
        ''' Verify if the execution can proceed due to the sleep time assigned '''
        try:
            with open(self.control_file) as infile:
                input_time = float(infile.read())
        except IOError:
            input_time = time.time() + 1
        except ValueError:
            input_time = time.time() + 1
        while (time.time() <= input_time):
            print "Could not execute now", str(datetime.datetime.now())
            print "Sleeping for {} seconds. Until {}".format(
                input_time - time.time(), datetime.datetime.fromtimestamp(input_time))
            time.sleep(input_time - time.time())

    def assign_next_execution(self):
        ''' Write the data of next execution for future verifications  '''
        output_time = open(self.control_file, "w")
        output_time.write(str(self.next_execution))
        output_time.flush()
        output_time.close()
        print "Execution took", str(self.duration), "seconds."
        print "Next will occur in", float(self.sleep_time) - self.duration, "seconds."
        print "Next will occur at", datetime.datetime.fromtimestamp(self.next_execution)
        time.sleep(self.sleep_time)

    def wait(self,seconds):
        time.sleep(seconds)
