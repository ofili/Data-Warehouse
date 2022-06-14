import boto3
import json
import os
import configparser
import logging
import logging.config
import time

# Setting up logger
logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

# Load cluster configuration from config file
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')



# Create IAM role
def create_iam_role(iam_client):
    role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")
    logger.info("Creating IAM role")
    try:
        response = iam_client.create_role(
            Path='/',
            RoleName='{}'.format(role_name),
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }]
                },
                indent=4,
                separators=(',', ': ')
            )
        )
        logger.debug(f"Response from create_role: {response}")
        logger.info(f"Role create response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['Role']['Arn']
    except Exception as e:
        logger.error(f"Error creating IAM role: {e}")
        return False
    
    # Attach policy to IAM role
    try:
        response = iam_client.attach_role_policy(
            RoleName='DWH_IAM_ROLE',
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        logger.debug(f"Response from attach_role_policy: {response}")
        logger.info(f"Policy attach response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    except Exception as e:
        logger.error(f"Error attaching policy to IAM role: {e}")
        return False

    # Get IAM role ARN
    try:
        response = iam_client.get_role(
            RoleName='DWH_IAM_ROLE'
        )
        logger.debug(f"Response from get_role: {response}")
        logger.info(f"Role ARN: {response['Role']['Arn']}")
        return response['Role']['Arn']
    except Exception as e:
        logger.error(f"Error getting IAM role ARN: {e}")
        return False

    return True if((response['ResponseMetadata']['HTTPStatusCode'] == 200) and (response['Role']['Arn'] is not None)) else False


# Create Redshift cluster
def create_redshift_cluster(redshift_client, role_arn, cluster_type, node_type, cluster_size, db_name, db_user, db_password):
    """
    Create a Redshift cluster
    :param redshift_client: an Redshift service client instance
    :param cluster_identifier: name of the cluster
    :param node_type: node type of the cluster
    :param db_name: name of the database
    :param db_user: name of the database user
    :param db_password: password of the database user
    :param cluster_type: type of the cluster
    :param cluster_size: size of the cluster
    :param iam_role_arn: IAM role ARN
    :return: True if cluster created successfully.
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

    # IAM role ARN
    iam_role = ('DWH', 'DWH_IAM_ROLE_NAME')

    try:
        response = redshift_client.create_cluster(
            ClusterIdentifier=cluster_identifier,
            DBName=db_name,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(cluster_size),
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            IamRoles=[iam_role]
        )
        logger.debug(f"Response from create_cluster: {response}")
        logger.info(f"Cluster create response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logger.error(f"Error creating Redshift cluster: {e}")
        return False

    # Get cluster properties
    try:
        response = redshift_client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )
        logger.debug(f"Response from describe_clusters: {response}")
        logger.info(f"Cluster properties: {response['Clusters'][0]['ClusterIdentifier']}")
        return response['Clusters'][0]['ClusterIdentifier']
    except Exception as e:
        logger.error(f"Error getting cluster properties: {e}")
        return False
    
    return True if((response['ResponseMetadata']['HTTPStatusCode'] == 200) and (response['Clusters'][0]['ClusterIdentifier'] is not None)) else False


# Create ec2 security group
def create_ec2_security_group(ec2_client, cluster_identifier):
    """
    Create an EC2 security group
    :param ec2_client: an EC2 service client instance
    :param cluster_identifier: name of the cluster
    :return: True if security group created successfully.
    """
    try:
        response = ec2_client.create_security_group(
            GroupName=cluster_identifier,
            Description='Security group for Redshift cluster'
        )
        logger.debug(f"Response from create_security_group: {response}")
        logger.info(f"Security group create response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']

        ec2_client.authorize_security_group_ingress(
            GroupName=cluster_identifier,
            IpProtocol='tcp',
            FromPort=int(port),
            ToPort=int(port),
            CidrIp='0.0.0.0/0',
        )
        logger.debug(f"Response from authorize_security_group_ingress: {response}")
        logger.info(f"Security group authorize response: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode']

    except Exception as e:
        logger.error(f"Error creating EC2 security group: {e}")
        return False


# 

def main():
    """
    Main function.
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


    # Create boto3 clients
    iam_client = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift_client = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2_client = boto3.client('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    # Create IAM role
    role_arn = create_iam_role(iam_client)

    # Create Redshift cluster
    cluster_arn = create_redshift_cluster(redshift_client, role_arn, cluster_type, node_type, cluster_size, db_name, db_user, db_password)

    # Create EC2 security group
    create_ec2_security_group(ec2_client, cluster_identifier)




if __name__ == '__main__':
    main()
