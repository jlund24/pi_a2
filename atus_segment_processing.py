import pandas
import datetime

'''
Read in segment data
Group by time segment
Calculate 2 most common activities

Make new dataframe
Columns: Type (atus_1/2, me_1/2), Segment ID, Segment Name ("12:00 AM"), Activity Code, Activity Name, Entry Count

'''
activity_code_to_name = {"01": "personal care", "02": "household activities", "03": "caring for household members", "04": "caring for nonhousehold members", "05": "work & work related activities", "06": "education", "07": "consumer purchases", "08": "professional & personal care services", "09": "household services", "10": "government services & civic obligations", "11": "eating and drinking", "12": "socializing, relaxing, and leisure", "13": "sports, exercise, and recreation", "14": "religious and spiritual activities", "15": "volunteer activities", "16": "telephone calls", "18": "traveling", "50": "no response"}
activity_code_mapping = {1: "personal care", 2: "household activities", 3: "caring for HH members", 4: "caring for nonHH members", 5: "work", 6: "education", 7: "consumer purchases", 8: "professional and personal care services", 9: "household services", 10: "government services and civic obligations", 11: "eating and drinking", 12: "socializing, relaxing, and leisure", 13: "sports, exercise, and recreation", 14: "religious and spiritual activities", 15: "volunteer activities", 16: "telephone calls", 18: "traveling", 50: "no response"}

toggl_data = pandas.read_csv("data/Toggl_time_entries_2021-02-01_to_2021-03-31.csv", header=0, usecols=["Description", "Start date", "Start time", "End date", "End time", "Tags"])



toggl_segmented_df = pandas.DataFrame(columns=('Segment ID', 'Activity Code', 'Description', 'DayOfWeek', 'DayType'))

# make segments list
segments = []
seg_start = datetime.datetime.strptime("00:00:00", "%H:%M:%S")
last_seg = datetime.datetime.strptime("23:45:00", "%H:%M:%S")
while seg_start <= last_seg:
    seg_end = seg_start + datetime.timedelta(minutes=15)
    segments.append({"start": seg_start.time(), "end": seg_end.time()})
    seg_start = seg_end

segments[-1]["end"] = datetime.datetime.strptime("23:59:59", "%H:%M:%S").time()

def convertTagToActivityCode(tag):
    position = list(activity_code_mapping.values()).index(tag)
    return list(activity_code_mapping.keys())[position]

num_to_weekday = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}
num_to_daytype = {1: "Workday", 2: "Workday", 3: "Workday", 4: "Workday", 5: "Workday", 6: "Weekend", 7: "Weekend"}

def convertDateStringToDayOfWeek(date_string):
    date = datetime.datetime.fromisoformat(date_string)
    # print(date.isoweekday())
    return {"weekday": num_to_weekday[date.isoweekday()], "daytype": num_to_daytype[date.isoweekday()]}


def roundDown(time):
    rounded_time = time - datetime.timedelta(minutes=time.minute % 15, seconds=time.second, microseconds=time.microsecond)
    return rounded_time


def roundUp(time):
    # print(datetime.timedelta(minutes=15 - (time.minute % 15 if time.minute % 15 != 0 else 15),
    #                                  seconds=time.second, microseconds=time.microsecond))
    rounded_time = time + datetime.timedelta(minutes=15 - (time.minute % 15 if time.minute % 15 != 0 else 15),
                                     seconds=time.second, microseconds=time.microsecond)

    return rounded_time


def getStartSegmentIndex(start_time_string):
    start_time = datetime.datetime.strptime(start_time_string, "%H:%M:%S")
    rounded_start_time = roundDown(start_time)
    for i, d in enumerate(segments):
        if d["start"] == rounded_start_time.time():
            return i


def getEndSegmentIndex(end_time_string):
    end_time = datetime.datetime.strptime(end_time_string, "%H:%M:%S").replace(second=0, microsecond=0)

    rounded_end_time = roundUp(end_time)

    end_midnight_segment = datetime.datetime.strptime("23:59:59", "%H:%M:%S").time()
    end_midnight_full = datetime.datetime.strptime("00:00:00", "%H:%M:%S").time()
    for i, d in enumerate(segments):
        if d["end"] == rounded_end_time.time():
            return i
        elif d["end"] == end_midnight_segment and rounded_end_time.time() == end_midnight_full:
            return i


def addEntriesToSegmented(row):
    global toggl_segmented_df
    entry_start_datetime = datetime.datetime.strptime(row["Start time"], "%H:%M:%S")  # .time()
    entry_end_datetime = datetime.datetime.strptime(row["End time"], "%H:%M:%S")  # .time()

    first_segment = getStartSegmentIndex(row["Start time"])
    last_segment = getEndSegmentIndex(row["End time"])

    activity_code = convertTagToActivityCode(row["Tags"])
    start_day_data = convertDateStringToDayOfWeek(row["Start date"])
    end_day_data = convertDateStringToDayOfWeek(row["End date"])

    if first_segment <= last_segment:
        # do things normally
        # add segment entry for each index it covers
        # 'Segment ID', 'Activity Code', 'Description', 'DayOfWeek', 'DayType'
        for i in range(first_segment, last_segment):
            toggl_segmented_df = toggl_segmented_df.append(pandas.Series({"Segment ID": i,
                                                              "Description": row["Description"],
                                                              "Activity Code": activity_code,
                                                              "DayOfWeek": start_day_data["weekday"],
                                                              "DayType": start_day_data["daytype"]
                                                                    }),
                                               ignore_index=True)

    else:
        for i in range(first_segment, len(segments)):
            toggl_segmented_df = toggl_segmented_df.append(pandas.Series({"Segment ID": i,
                                                                        "Description": row["Description"],
                                                                        "Activity Code": activity_code,
                                                                        "DayOfWeek": start_day_data["weekday"],
                                                                        "DayType": start_day_data["daytype"]
                                                                        }),
                                               ignore_index=True)

        for i in range(0, last_segment):
            toggl_segmented_df = toggl_segmented_df.append(pandas.Series({"Segment ID": i,
                                                                          "Description": row["Description"],
                                                                          "Activity Code": activity_code,
                                                                          "DayOfWeek": end_day_data["weekday"],
                                                                          "DayType": end_day_data["daytype"]
                                                                          }),
                                               ignore_index=True)


print(convertTagToActivityCode(toggl_data["Tags"][0]))
print(convertDateStringToDayOfWeek(toggl_data["Start date"][0]))
print(getStartSegmentIndex(toggl_data["Start time"][0]))
print(getEndSegmentIndex(toggl_data["End time"][0]))

toggl_data.apply(lambda row: addEntriesToSegmented(row), axis=1)
print(toggl_segmented_df.head(20))

toggl_segmented_df.to_csv("toggle_segmented_data.csv")