import time

import boto3
import json
import configparser
import logging

import pandas as pd


# Load cluster configuration from config file
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")



# Create clients for EC2, S3, IAM, and Redshift
ec2 = boto3.resource('ec2',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )
s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )
iam = boto3.client('iam',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )
redshift = boto3.client('redshift',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

# Setting up logger


# Create the IAM role
def create_iam_role(DWH_IAM_ROLE_NAME):
    """
    This function will create the IAM role.
    """
    try:
        # Create the IAM role
        print("Creating IAM role...")
        # iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }],
                    'Version': '2012-10-17'
                }
            )
        )
        print(f"IAM role created successfully.")
        # Attach policy
        print("Attaching policy...")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")[
            'ResponseMetadata']['HTTPStatusCode']
        print("Get the IAM role ARN")
        # Get and print the IAM role ARN
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(f"IAM role ARN: {role_arn}" + "\n" + "-" * 50 + "\n")
        return role_arn
    except Exception as e:
        print(e)
        print(f"IAM not created." + str(e) + "\n" + "-" * 50 + "\n")


def create_redshift_cluster(DWH_IAM_ROLE_NAME, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER,
                            DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD):
    """
    This function will create the redshift cluster.
    """
    
    try:
        # Create the redshift cluster
        print("Creating redshift cluster...")
        # redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        response = redshift.create_cluster(
            # Hardware configuration parameters
            ClusterType=DWH_CLUSTER_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            NodeType=DWH_NODE_TYPE,

            # Identifiers & access credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles for s3 access
            IamRoles = [role_arn]
        )
        print(f"Redshift cluster created successfully." + "\n" + "-" * 50 + "\n")

        # Check if cluster is available
        print("Checking if cluster is available...")
        while True:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0][
                'ClusterStatus']
            if response == 'available':
                print(f"Cluster is available.")
                break
            else:
                print(f"Cluster is not available. Waiting...")
                time.sleep(5)
    except Exception as e:
        print(e)
        print(f"Redshift cluster not created." + str(e) + "\n" + "-" * 50 + "\n")


def prettyRedshiftProps(props):
    """
    This function will pretty print the redshift cluster properties.
    """
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                "NumberOfNodes", "VpcId"]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def main():
    """
    This function will create the IAM role, redshift cluster, and the redshift cluster security group.
    """

    # Create a new IAM role
    role_arn = create_iam_role(DWH_IAM_ROLE_NAME)

    # Create a new redshift cluster
    #create_redshift_cluster(DWH_IAM_ROLE_NAME, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD)
    try:
        # Create the redshift cluster
        print("Creating redshift cluster...")
        # redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        response = redshift.create_cluster(
            # Hardware configuration parameters
            ClusterType=DWH_CLUSTER_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            NodeType=DWH_NODE_TYPE,

            # Identifiers & access credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles for s3 access
            IamRoles = [role_arn]
        )
        print(f"Redshift cluster created successfully." + "\n" + "-" * 50 + "\n")

        # Check if cluster is available
        print("Checking if cluster is available...")
        while True:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0][
                'ClusterStatus']
            if response == 'available':
                print(f"Cluster is available.")
                break
            else:
                print(f"Cluster is not available. Waiting...")
                time.sleep(5)
    except Exception as e:
        print(e)
        print(f"Redshift cluster not created." + str(e) + "\n" + "-" * 50 + "\n")

    # Get the redshift cluster properties
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT: {}".format(DWH_ENDPOINT))
    print("DWH_ROLE_ARN: {}".format(DWH_ROLE_ARN))
    print("-" * 50 + "\n")

    # Open an incoming  TCP port to access the cluster endpoint
    ''' try:
        print("Opening TCP port...")
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(f"Default security group: {defaultSg}")
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        print("VPC and subnets created successfully." + "\n" + "-" * 50 + "\n")
    except Exception as e:
        print(e)
        print(f"VPC and subnets not created." + str(e) + "\n" + "-" * 50 + "\n") '''


if __name__ == '__main__':
    main()
