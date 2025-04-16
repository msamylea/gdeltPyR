#!/usr/bin/python

# Author:
# Linwood Creekmore
# Email: valinvescap@gmail.com


##################################
# Standard library imports
##################################
import datetime
import sys
import traceback

##################################
# Third party imports
##################################
import numpy as np
from dateutil.parser import parse


def _date_input_check(date):
    """Function to check date entered by user.

    Parameters
    ----------
        date : {string or list},
            Input data, where ``date`` is a single date string,
            two dates representing a range, or several dates
            that represent individual days of interest.


    Returns
    -------
    self : None
        Returns self.
        :param date:
    """

    if isinstance(date, str):

        if date != "":
            if parse(date) > datetime.datetime.now():  # pragma: no cover
                raise ValueError(
                    'Your date is greater than the current date. '
                    'Please enter a relevant date.'
          
            )


    elif isinstance(date, list) or isinstance(date, np.ndarray):
        newdate =[]

        for l in date:
            if len(l) == 4:
                test = (str(datetime.datetime.strptime(l, '%Y')))
                newdate.append(test)
            elif len(l) == 6:  # pragma: no cover
                test = str(datetime.datetime.strptime(l, '%Y%m'))
                newdate.append(test)
            else:
                try:
                    test = str(parse(str(l)))
                except:  # pragma: no cover
                    test = l
                newdate.append(test)
            if parse(test) < parse('Feb 18 2015'):
                raise ValueError(
                    "GDELT 2.0 only supports \'Feb 18 2015 - "
                    "Present\'queries currently. Try another date."
                )
        date = newdate

        if len(date) == 1:
            try:
                if parse("".join(date)) > datetime.datetime.now():
                    raise ValueError(
                        'Your date is greater than the current date.  '
                        'Please enter a relevant date.'
                    )
               
            except:
                # exc_type, exc_value, exc_traceback = sys.exc_info()
                # traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                # traceback.print_exception(exc_type, exc_value, exc_traceback,
                #                           limit=2, file=sys.stdout)

                raise ValueError(
                    "One or more of your input date strings does not parse to "
                    "a date format. Check input."
                )
            # if datetime.datetime.now().hour <= 6 and parse(
            #         "".join(date)).date() == (
            #             datetime.datetime.now().date() - datetime.timedelta(
            #             days=1)):
            #     raise BaseException('GDELT 1.0 posts the latest daily update '
            #                         'by 6AM EST. The next update will appear '
            #                         'in {0}'.format(str(
            #         datetime.datetime.combine(
            #             datetime.datetime.now(), datetime.datetime.min.time()
            #         ) + datetime.timedelta(hours=6, minutes=00, seconds=00) -
            #         datetime.datetime.now())))
            #
            # return "".join(date)


        elif len(date) == \
                2 and (isinstance(date, list) or isinstance(date, np.ndarray)):

            try:
                list(map(parse, date))
            except Exception as exc:  # pragma: no cover
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                          limit=2, file=sys.stdout)
                raise ValueError(
                    "One or more of your input date strings does not parse to "
                    "a date format. Check input."
                )

            if not bool(parse(date[0]) < parse(date[1])):
                raise ValueError(
                    'Start date greater than or equal to end date. '
                    'Check your entered date query.'
                )

            elif not np.all(
                    np.logical_not(np.array(list(map(parse, date))) >
                                           datetime.datetime.now())
            ):
                raise ValueError(
                    "One of your dates is greater than the current date. "
                    "Check your entered date query."
                )
         

        elif len(date) > 2:

            try:
                map(parse, date)
            except Exception as exc:  # pragma: no cover
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                          limit=2, file=sys.stdout)
                raise ValueError(
                    "One or more of your input date strings does not parse "
                    "to a date format. Check input."
                )

            if not np.all(
                    np.logical_not(np.array(list(map(parse, date))) >
                                           datetime.datetime.now())
            ):
                raise ValueError(
                    "One or more of your input date strings is greater than"
                    " today's date. Check input."
                )

            elif np.any(
                    np.logical_not(np.array(list(map(parse, date)
                                                 )) > parse("Feb 18 2015"))) == \
                    True:  # pragma: no cover
                raise ValueError(
                    "GDELT 2.0 only supports \'Feb 18 2015 - Present\'queries "
                    "currently. Try another date."
                )
         