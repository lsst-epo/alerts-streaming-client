import datetime
import julian

class ModifiedJulianDateUtil():

    # Format of input must be in the following format:
    #
    # MMDDYYYY
    #
    # Example: 07112022
    def validate_and_convert_input_date(pre_date):
        if isinstance(pre_date, int):
            pre_date = str(pre_date)

        month = pre_date[0:2]
        day = pre_date[2:4]
        year = pre_date[4:]
        post_date = year + "-" + month + "-" + day
        d = datetime.fromisoformat(post_date)

        return round(julian.to_jd(d, fmt="mjd"))

    # mjd: modified julian date
    def get_date_in_mjd():
        return round(julian.to_jd(datetime.datetime.today(), fmt="mjd"))