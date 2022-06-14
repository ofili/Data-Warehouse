import configparser
import logging.config
import time

import boto3

# Setting up logger
from redshift_setup import get_cluster_status

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

# Load cluster configuration from config file
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

# Detach policy
def detach_policy(iam_client, role_name, policy_arn):
    """
    Detach a policy from an IAM role
    :param iam_client: an IAM service client instance
    :param role_name: name of the role
    :param policy_arn: ARN of the policy
    :return: None
    """
    role_name = config.get('DWH', 'DWH_IAM_ROLE_NAME')
    try:
        iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        logger.info("Detached policy from role")
    except Exception as e:
        logger.error(f"Error detaching policy from role: {e}")

# Delete IAM role
def delete_iam_role(iam_client, role_name):
    """
    Delete an IAM role
    :param iam_client: an IAM service client instance
    :param role_name: name of the role
    :return: True if role deleted successfully.
    """
    role_name = config.get('DWH', 'DWH_IAM_ROLE_NAME')
    try:
        response = iam_client.delete_role(
            RoleName=role_name
        )
        logger.debug(f"Response from delete_iam_role: {response}")
        logger.info(f"IAM role delete response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logger.error(f"Error deleting IAM role: {e}")
        return False


# Delete EC2 security group
def delete_security_group(ec2_client):
    """
    Delete an EC2 security group
    :param ec2_client: an EC2 service client instance
    :return: True if security group deleted successfully.
    """
    group_name = config.get('DWH', 'DWH_SECURITY_GROUP')
    try:
        response = ec2_client.delete_security_group(
            GroupName=group_name
        )
        logger.debug(f"Response from delete_security_group: {response}")
        logger.info(f"Security group delete response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logger.error(f"Error deleting EC2 security group: {e}")
        return False


# Delete cluster
def delete_cluster(redshift_client, cluster_identifier):
    """
    Delete a Redshift cluster
    :param redshift_client: a Redshift service client instance
    :param cluster_identifier: name of the cluster
    :return: True if cluster deleted successfully.
    """
    if len(redshift_client.describe_clusters()['Clusters']) == 0:
        logger.info(f"Redshift cluster {cluster_identifier} does not exist")
        return True
    try:
        while not get_cluster_status(redshift_client, cluster_identifier=cluster_identifier):
            logger.info("Can't delete cluster. Waiting for cluster to become ACTIVE")
            time.sleep(10)
        response = \
            redshift_client.delete_cluster(
                ClusterIdentifier=cluster_identifier,
                SkipFinalClusterSnapshot=True
            )
        logger.debug(f"Response from delete_cluster: {response}")
        logger.info(f"Cluster delete response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logger.error(f"Error deleting Redshift cluster: {e}")
        return False


def main():
    """
    Main function
    """
    # Cluster hardware config
    cluster_type = config.get('DWH', 'DWH_CLUSTER_TYPE')
    cluster_size = config.get('DWH', 'DWH_NUM_NODES')
    node_type = config.get('DWH', 'DWH_NODE_TYPE')

    # Cluster identifiers and credentials
    cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    db_name = config.get('DWH', 'DWH_DB')
    db_user = config.get('DWH', 'DWH_DB_USER')
    db_password = config.get('DWH', 'DWH_DB_PASSWORD')
    port = config.get('DWH', 'DWH_PORT')
    role_name = config.get('DWH', 'DWH_IAM_ROLE_NAME')

    # Create boto3 clients
    iam_client = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift_client = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET)
    ec2_client = boto3.client('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    # Detach policy
    logger.info(f"Detaching policy from role: {role_name}")
    detach_policy(iam_client, role_name, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    # Delete IAM role
    logger.info(f"Deleting IAM role: {role_name}")
    delete_iam_role(iam_client, role_name)

    # Delete ec2 security group
    logger.info(f"Deleting EC2 security group: {ec2_client}")
    if delete_security_group(ec2_client):
        logger.info(f"EC2 security group deleted successfully")
    else:
        logger.error(f"Error deleting EC2 security group")
        return False

    # Delete cluster
    logger.info(f"Deleting Redshift cluster: {cluster_identifier}")
    if delete_cluster(redshift_client, cluster_identifier):
        logger.info(f"Redshift cluster deleted successfully")
    else:
        logger.error(f"Error deleting Redshift cluster")
        return False


if __name__ == '__main__':
    main()
