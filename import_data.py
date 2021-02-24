import pandas

activity_code_to_name = {"01": "personal care", "02": "household activities", "03": "caring for household members", "04": "caring for nonhousehold members", "05": "work & work related activities", "06": "education", "07": "consumer purchases", "08": "professional & personal care services", "09": "household services", "10": "government services & civic obligations", "11": "eating and drinking", "12": "socializing, relaxing, and leisure", "13": "sports, exercise, and recreation", "14": "religious and spiritual activities", "15": "volunteer activities", "16": "telephone calls", "18": "traveling", "50": "no response"}

def convertCodeToName(activity_code):
    return activity_code_to_name[activity_code]


data = pandas.read_csv("data/atusact-2019/atusact_2019.csv", header=0, usecols=["TUSTARTTIM", "TUSTOPTIME", "TUTIER1CODE"], converters={"TUTIER1CODE": convertCodeToName})
print(data.head(100))
