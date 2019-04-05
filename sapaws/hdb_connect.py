# -*- coding: utf-8 -*-
"""SAP HANA DB connection context manager."""

import pyhdb


class HDBConnect(object):
    """HDBConnect class is a HANA DB context manager."""

    def __init__(self, **connection):
        """Init dunder method."""
        """
            The connection args need to match the format and keys for
            the pyHDB connection parameters.

            An example:
                {
                    address='clkhdb01.lab.clockwork.ca',
                    port=30013,
                    user='SYSTEM',
                    password='#CLKhana001a#',
                    database='SYSTEMDB'
                }
        """
        self.connection = pyhdb.connect(**connection)

    def __enter__(self):
        """Enter dunder method."""
        return self.connection.cursor()

    def __exit__(self, type, value, traceback):
        """Exit dunder method."""
        self.connection.close()
