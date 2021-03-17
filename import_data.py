import pandas
import datetime
import time as time_lib
from tqdm import tqdm

activity_code_to_name = {"01": "personal care", "02": "household activities", "03": "caring for household members", "04": "caring for nonhousehold members", "05": "work & work related activities", "06": "education", "07": "consumer purchases", "08": "professional & personal care services", "09": "household services", "10": "government services & civic obligations", "11": "eating and drinking", "12": "socializing, relaxing, and leisure", "13": "sports, exercise, and recreation", "14": "religious and spiritual activities", "15": "volunteer activities", "16": "telephone calls", "18": "traveling", "50": "no response"}
segmented_df = pandas.DataFrame(columns=('Segment ID', # "Segment Range", 'Start', 'End',
                                         'Activity Code'))

def convertCodeToName(activity_code):
    return activity_code_to_name[activity_code]


def roundDown(time):
    rounded_time = time - datetime.timedelta(minutes=time.minute % 15, seconds=time.second, microseconds=time.microsecond)
    return rounded_time


def roundUp(time):
    rounded_time = time + datetime.timedelta(minutes=15 - (time.minute % 15 if time.minute % 15 != 0 else 15),
                                     seconds=time.second, microseconds=time.microsecond)
    return rounded_time


def getStartSegmentIndex(start_time):
    rounded_start_time = roundDown(start_time)
    for i, d in enumerate(segments):
        if d["start"] == rounded_start_time.time():
            return i


def getEndSegmentIndex(end_time):
    rounded_end_time = roundUp(end_time)
    end_midnight_segment = datetime.datetime.strptime("23:59:59", "%H:%M:%S").time()
    end_midnight_full = datetime.datetime.strptime("00:00:00", "%H:%M:%S").time()
    for i, d in enumerate(segments):
        if d["end"] == rounded_end_time.time():
            return i
        elif d["end"] == end_midnight_segment and rounded_end_time.time() == end_midnight_full:
            return i

def addEntriesToSegmented(start_time, end_time, activity_code):
    global segmented_df
    entry_start_datetime = datetime.datetime.strptime(start_time, "%H:%M:%S")  # .time()
    entry_end_datetime = datetime.datetime.strptime(end_time, "%H:%M:%S")  # .time()

    first_segment = getStartSegmentIndex(entry_start_datetime)
    last_segment = getEndSegmentIndex(entry_end_datetime)

    if first_segment <= last_segment:
        # do things normally
        # add segment entry for each index it covers
        for i in range(first_segment, last_segment):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              #"Segment Range": "{}-{}".format(segments[i]["start"],
                                                                            #                  segments[i]["end"]),
                                                              "Activity Code": activity_code}),
                                                              #"Start": entry_start_datetime.time(),
                                                              #"End": entry_end_datetime.time()}),
                                               ignore_index=True)

    else:
        for i in range(first_segment, len(segments)):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              # "Segment Range": "{}-{}".format(segments[i]["start"],
                                                              #                                 segments[i]["end"]),
                                                              "Activity Code": activity_code}),
                                                              # "Start": entry_start_datetime.time(),
                                                              # "End": entry_end_datetime.time()}),
                                               ignore_index=True)

        for i in range(0, last_segment):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              # "Segment Range": "{}-{}".format(segments[i]["start"],
                                                              #                                 segments[i]["end"]),
                                                              "Activity Code": activity_code}),
                                                              # "Start": entry_start_datetime.time(),
                                                              # "End": entry_end_datetime.time()}),
                                               ignore_index=True)

data = pandas.read_csv("data/atusact-2019/atusact_2019.csv", header=0, usecols=["TUSTARTTIM", "TUSTOPTIME", "TUTIER1CODE"])#, converters={"TUTIER1CODE": convertCodeToName})
print(data.head(100))

data.columns = ["start", "end", "activity_code"]

segments = []
seg_start = datetime.datetime.strptime("00:00:00", "%H:%M:%S")
last_seg = datetime.datetime.strptime("23:45:00", "%H:%M:%S")
while seg_start <= last_seg:
    seg_end = seg_start + datetime.timedelta(minutes=15)
    segments.append({"start": seg_start.time(), "end": seg_end.time()})
    seg_start = seg_end

segments[-1]["end"] = datetime.datetime.strptime("23:59:59", "%H:%M:%S").time()
# print(segments)
date_obj = datetime.datetime.strptime("00:00:00", "%H:%M:%S").time()

print(segments)
output_string = "{"
for index, segment in enumerate(segments):
    output_string += "{}: '{}',".format(index, str(segment["start"]))

print(output_string + "}")
start = time_lib.time()

tqdm.pandas()

# Now you can use `progress_apply` instead of `apply`
# data.groupby(0).progress_apply(lambda x: x**2)
data.head(150000).progress_apply(lambda row: addEntriesToSegmented(row["start"], row["end"], row["activity_code"]), axis=1)
print(time_lib.time() - start)
print(segmented_df.head(20))
grouped_by_segment = segmented_df.groupby('Segment ID')

print(grouped_by_segment.get_group(0))
data.to_csv("full_data.csv")
segmented_df.to_csv("segmented_data.csv")
grouped_by_segment.to_csv("grouped_data.csv")
raise Exception()

for df_ind in data.index:
    if df_ind % 1000 == 0:
        print("Processing entry #: {}".format(df_ind))
        print("Elapsed time: {} - Average time per 1k entries: {}".format(time_lib.time() - start, (time_lib.time() - start) / (df_ind + 1) * 1000))
    entry_start_datetime = datetime.datetime.strptime(data['start'][df_ind], "%H:%M:%S")#.time()
    entry_end_datetime = datetime.datetime.strptime(data['end'][df_ind], "%H:%M:%S")#.time()
    # print(entry_start_datetime)
    # print(getStartSegmentIndex(entry_start_datetime))
    # print(entry_end_datetime)
    # print(getEndSegmentIndex(entry_end_datetime))
    first_segment = getStartSegmentIndex(entry_start_datetime)
    last_segment = getEndSegmentIndex(entry_end_datetime)

    if first_segment <= last_segment:
        # do things normally
        # add segment entry for each index it covers
        for i in range(first_segment, last_segment):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              "Segment Range": "{}-{}".format(segments[i]["start"],
                                                                                              segments[i]["end"]),
                                                              "Activity Code": data['activity_code'][df_ind],
                                                              "Start": entry_start_datetime.time(),
                                                              "End": entry_end_datetime.time()}),
                                               ignore_index=True)
    else:
        for i in range(first_segment, len(segments)):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              "Segment Range": "{}-{}".format(segments[i]["start"],
                                                                                              segments[i]["end"]),
                                                              "Activity Code": data['activity_code'][df_ind],
                                                              "Start": entry_start_datetime.time(),
                                                              "End": entry_end_datetime.time()}),
                                               ignore_index=True)

        for i in range(0, last_segment):
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": i,
                                                              "Segment Range": "{}-{}".format(segments[i]["start"],
                                                                                              segments[i]["end"]),
                                                              "Activity Code": data['activity_code'][df_ind],
                                                              "Start": entry_start_datetime.time(),
                                                              "End": entry_end_datetime.time()}),
                                               ignore_index=True)

print(segmented_df.head(20))
grouped_by_segment = segmented_df.groupby('Segment ID')

print(grouped_by_segment.get_group(0))

data.to_csv("full_data.csv")
segmented_df.to_csv("segmented_data.csv")
grouped_by_segment.to_csv("grouped_data.csv")
# raise Exception("stop")



for seg_index, segment in enumerate(segments):
    print("Processing segment #: {} - {}-{}".format(seg_index, segment["start"], segment["end"]))

    for df_ind in data.index:
        # if df_ind > 100:
        #     break
        add = False
        entry_start = datetime.datetime.strptime(data['start'][df_ind], "%H:%M:%S").time()
        entry_end = datetime.datetime.strptime(data['end'][df_ind], "%H:%M:%S").time()

        # normal time entry
        if entry_start < entry_end:
            if entry_start < segment["end"] <= entry_end:
                add = True
            elif entry_start <= segment["start"] < entry_end:
                add = True
            elif entry_start <= segment["start"] and entry_end >= segment["end"]:
                add = True
            elif entry_start >= segment["start"] and entry_end <= segment["end"]:
                add = True
        # wraparound entry
        else:
            # split entry into two? start to 23:59:59 then 00:00:00 to end?
            entry_1_start = entry_start
            entry_1_end = datetime.datetime.strptime("23:59:59", "%H:%M:%S").time()
            entry_2_start = datetime.datetime.strptime("00:00:00", "%H:%M:%S").time()
            entry_2_end = entry_end
            if entry_1_start < segment["end"] <= entry_1_end:
                add = True
            elif entry_1_start <= segment["start"] < entry_1_end:
                add = True
            elif entry_1_start <= segment["start"] and entry_1_end >= segment["end"]:
                add = True
            elif entry_1_start >= segment["start"] and entry_1_end <= segment["end"]:
                add = True

            if entry_2_start < segment["end"] <= entry_2_end:
                add = True
            elif entry_2_start <= segment["start"] < entry_2_end:
                add = True
            elif entry_2_start <= segment["start"] and entry_2_end >= segment["end"]:
                add = True
            elif entry_2_start >= segment["start"] and entry_2_end <= segment["end"]:
                add = True

        if add:
            segmented_df = segmented_df.append(pandas.Series({"Segment ID": seg_index,
                "Segment Range": "{}-{}".format(segment["start"], segment["end"]),
                "Activity Code": data['activity_code'][df_ind], "Start": entry_start, "End": entry_end}),
                                               ignore_index=True)

print(segmented_df.head(20))
grouped_by_segment = segmented_df.groupby('Segment ID')

print(grouped_by_segment.get_group(0))

data.head.to_csv("full_data.csv")
segmented_df.to_csv("segmented_data.csv")
grouped_by_segment.to_csv("grouped_data.csv")

'''
For each 15 minute segment of the day 00:00 - 00:15, etc. what was the most common activity?
For each row in df
    if time segment start is after entry start and before entry end, it's in
    if segment end if after entry start and before end, it's in
    if segment start is before entry start and segment end is after entry end, it's in
'''
