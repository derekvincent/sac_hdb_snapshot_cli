# -*- coding: utf-8 -*-
"""AWS Waiters config."""

import botocore.waiter


class Waiters:
    """AWS Waiters."""

    def __init__(self):
        """Init Dunder method."""
        pass

    @staticmethod
    def ssm_command_invocation_waiter():
        """Return the ListCommandInvokcations waiter."""
        return botocore.waiter.WaiterModel({
            "version": 2,
            "waiters": {
                "ssmCommandInvocations": {
                    "delay": 15,
                    "operation": "ListCommandInvocations",
                    "maxAttempts": 40,
                    "acceptors": [
                        {
                            "expected": "Success",
                            "matcher": "pathAll",
                            "state": "success",
                            "argument": "CommandInvocations[].Status"
                        },
                        {
                            "expected": "Failed",
                            "matcher": "pathAny",
                            "state": "failure",
                            "argument": "CommandInvocations[].Status"
                        }
                    ]
                }
            }
        })
