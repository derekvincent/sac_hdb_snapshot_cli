# -*- coding: utf-8 -*-
"""SAP on AWS HANA DB snapshot manager."""

from hdb_connect import HDBConnect
from time import sleep
from aws_waiters import Waiters

import botocore.waiter
import botocore.exceptions
import boto3
import logging


logger = logging.getLogger('sac-hdb-snapshot')
debug = logging.debug

"""Customer AWS Waiter Models"""
waiterModel = Waiters.ssm_command_invocation_waiter()


class SnapshotManager:
    """SAP on AWS HANA DB snapshot manager."""

    def __init__(self, aws_instance_id):
        """Init dunder method."""
        pass

    def get_hana_status(self, **connection):
        """Get the Status of the HANA DB."""
        with HDBConnect(**connection) as hanaSql:
            hanaSql.execute("select * from M_SYSTEM_OVERVIEW")
            results = hanaSql.fetchall()
            status = 'Error'

            for result in results:
                if result['SECTION'] == 'Services' and result['NAME'] == 'All Started':
                    status = result['STATUS']

            return status

    def current_hana_snapshot_id(self, **connection):
        """Fetch the current prepared snapshot ID."""
        snapshot_status = \
            "select * from M_BACKUP_CATALOG where \"ENTRY_TYPE_NAME\"= 'data snapshot' and \"STATE_NAME\" = 'prepared'"
        backup_id = None

        with HDBConnect(**connection) as hanaSql:
            hanaSql.execute(snapshot_status)
            backup_id = hanaSql.fetchone()['BACKUP_ID']

        return backup_id

    def start_hana_snapshot(self, comment=None, **connection):
        """Trigger a HANA DB snapshot."""
        logger.debug('Entering --> start_hana_snapshot')

        snapshot_sql = "BACKUP DATA FOR FULL SYSTEM CREATE SNAPSHOT COMMENT '{}'".format(comment)
        snapshot_status = \
            "select * from M_BACKUP_CATALOG where \"ENTRY_TYPE_NAME\"= 'data snapshot' and \"STATE_NAME\" = 'prepared'"
        backup_id = None

        with HDBConnect(**connection) as hana_sql:
            result = hana_sql.execute(snapshot_sql)

            if result:
                hana_sql.execute(snapshot_status)
                backup_id = hana_sql.fetchone()['BACKUP_ID']

        logger.debug('Hana snapshot backup with ID: %s created', str(backup_id))

        return backup_id

    def confirm_hana_snapshot(self, backup_id, external_id, **connection):
        """Successfully confirm the HANA Snapshot has been taken."""
        logger.debug('Entering --> confirm_hana_snapshot')
        confirm_snapshot_sql = \
            "backup data for full system close snapshot BACKUP_ID {} successful '{}'".format(backup_id, external_id)
        with HDBConnect(**connection) as hanaSql:
            result = hanaSql.execute(confirm_snapshot_sql)
            return result

    def abandon_hana_snapshot(self, backup_id, comment='Snapshot Failed: Do not use', **connection):
        """Abandon the HANA Snapshot so it is not used."""
        logger.debug('Entering --> abandon_hana_snapshot')
        abandon_snapshot_sql = \
            "backup data for full system close snapshot BACKUP_ID {} unsuccessful '{}'".format(backup_id, comment)

        with HDBConnect(**connection) as hanaSql:
            result = hanaSql.execute(abandon_snapshot_sql)
            return result

    """
        AWS Functions
    """

    def list_data_ebs_volumes(self, instance_id, ebs_tag):
        """List all EBS volumes related to the HANA Data files."""
        """
            AWS Section to determining the EBS volumes to be used used:

            1) Leverage an AWS tag [ie. HANA-Data] on the specific instances to determine EBS disk used
               in the /hana/data mount point.
            2) Peek into the systems and figure out from the mount point back to the bock devices
        """

        ec2 = boto3.Session().resource('ec2')
        ec2Hana = ec2.Instance(instance_id)

        ebs_list = ec2Hana.volumes.filter(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': ['HANA-Data']
                }
            ]
        )

        hana_ebs_list = []
        for ebs in ebs_list:
            hana_data_ebs = {'volume-id': ebs.volume_id, 'tags': ebs.tags}
            for attachment in ebs.attachments:
                hana_data_ebs['device'] = attachment['Device']
                hana_data_ebs['state'] = attachment['State']

            hana_ebs_list.append(hana_data_ebs)

        return hana_ebs_list

    def start_hana_data_ebs_snapshot(self, instance_id, hana_data_snapshot_id, aws_snapshot_name, hana_name_tag="HANA-Data"):
        """Start snapshots of the Hana data EBS."""
        logger.debug('Entering --> start_hana_data_ebs_snapshot')

        ec2 = boto3.Session().resource('ec2')
        hana_db = ec2.Instance(instance_id)

        hana_data_ebs_list = hana_db.volumes.filter(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [hana_name_tag]
                }
            ]
        )

        logger.debug('%d EBS volumes found with HANA-Data Tag', len(list(hana_data_ebs_list)))
        ebs_snapshots = []

        for hana_data_ebs in hana_data_ebs_list:
            logger.debug('Volume %s will have snapshot created.', hana_data_ebs.id)
            snapshot_tags = [
                {'Key': 'Name', 'Value': hana_name_tag},
                {'Key': 'HANA-SnapshotId', 'Value': str(hana_data_snapshot_id)},
                {'Key': 'HANA-Device', 'Value': hana_data_ebs.attachments[0]['Device']}
            ]
            logger.debug('Volume %s snapshot Tag to be created: %s', hana_data_ebs.id, snapshot_tags)

            snapshot = hana_data_ebs.create_snapshot(
                Description='HANA on AWS Snapshot - {}'.format(hana_data_snapshot_id),
                TagSpecifications=[
                    {
                        'ResourceType': 'snapshot',
                        'Tags': snapshot_tags
                    }
                ]
            )
            logger.debug('Snapshot %s created for Volume %s', hana_data_ebs.id, snapshot.id)
            ebs_snapshots.append(snapshot.id)

        return ebs_snapshots

    def aws_snapshot_waiter(self, snapshots, delay=5, max_retry=120):
        """Wait for all EBS Snapshots to compete."""
        ec2_client = boto3.Session().client('ec2')
        retry_counter = 0

        while True:

            ec2_snapshots = ec2_client.describe_snapshots(SnapshotIds=snapshots)
            retry_counter += 1
            completed_snaps = 0

            for snapshot in ec2_snapshots['Snapshots']:
                if snapshot['State'] == 'completed':
                    completed_snaps += 1
                elif snapshot['State'] == 'error':
                    # TODO: Create own Exception
                    raise Exception('Snapshot Waiter', 'Error occured during snaphot')

            if len(ec2_snapshots['Snapshots']) == completed_snaps:
                logger.debug("All Snapshot have completed")
                break

            if retry_counter >= max_retry:
                # TODO: Create own Exception
                raise Exception('Snapshot Waiter', 'Max attempts exceeded')

            sleep(delay)

    def freeze_hana_data_fs(self, instance_id, hana_data_mount='/hana/data'):
        """Freeze Hana data volume."""
        logger.debug('Entering --> freeze_hana_data_fs')

        ssm_client = boto3.client('ssm')
        ssm_response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': ['fsfreeze --freeze {}'.format(hana_data_mount)]}
        )
        command_id = ssm_response['Command']['CommandId']
        logger.debug('SSM Command to freeze %s issued with command id --> %s', hana_data_mount, command_id)

        try:
            command_waiter = botocore.waiter.create_waiter_with_client('ssmCommandInvocations', waiterModel, ssm_client)
            command_waiter.wait(CommandId=command_id, InstanceId=instance_id)
        except botocore.exceptions.WaiterError as waiterError:
            logger.debug('Exception freezing filesystem %s: %s', hana_data_mount, waiterError)

    def thaw_hana_data_fs(self, instance_id, hana_data_mount='/hana/data'):
        """Thaw Hana data volume."""
        logger.debug('Entering --> thaw_hana_data_fs')
        ssm_client = boto3.client('ssm')
        ssm_response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': ['fsfreeze --unfreeze {}'.format(hana_data_mount)]}
        )
        command_id = ssm_response['Command']['CommandId']
        logger.debug('SSM Command to thaw %s issued with command id --> %s', hana_data_mount, command_id)

        try:
            command_waiter = botocore.waiter.create_waiter_with_client('ssmCommandInvocations', waiterModel, ssm_client)
            command_waiter.wait(CommandId=command_id, InstanceId=instance_id)
        except botocore.exceptions.WaiterError as waiterError:
            logger.debug('Exception thawing filesystem %s: %s', hana_data_mount, waiterError)

    def hana_snapshot(self, instance_id, comment, hana_data_tag, snapshot_tags ):
        """Public method for creation of a HANA snapshot in AWS."""
        pass
        """
            1. Check if 
        """
