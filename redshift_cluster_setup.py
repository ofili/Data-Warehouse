import boto3
import json
import configparser

def create_iam_role(DWH_IAM_ROLE_NAME):
    """
    Creates an IAM role for redshift cluster.
    """
    iam = boto3.client('iam',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    # Create IAM role
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    # Attach Policy
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


def main():
    """
    Main function.
    """
    config = configparser.ConfigParser()
    # config.read('dwh.cfg')
    config.read_file(open('dwh.cfg'))

    # Load  DWH parameters from config file
    KEY                         = config.get('AWS', 'KEY')
    SECRET                      = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE            = config.get('AWS', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES               = config.get('AWS', 'DWH_NUM_NODES')
    DWH_NODE_TYPE               = config.get('AWS', 'DWH_NODE_TYPE')

    DWH_CLUSTER_IDENTIFIER      = config.get('AWS', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB                      = config.get('AWS', 'DWH_DB')
    DWH_DB_USER                 = config.get('AWS', 'DWH_DB_USER')
    DWH_DB_PASSWORD             = config.get('AWS', 'DWH_DB_PASSWORD')
    DWH_PORT                    = config.get('AWS', 'DWH_PORT')

    DWH_IAM_ROLE_NAME           = config.get("DWH", "DWH_IAM_ROLE_NAME")

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

    # Create IAM role
    roleArn = create_iam_role(DWH_IAM_ROLE_NAME)
    print(roleArn)

    # Create Redshift cluster
    try:
        print("2.1 Create Redshift Cluster")
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]  
        )

        # Open an incoming TCP port to access the endpoint
        vcp = ec2.Vcp(id = myClusterProps['VcpId'])
        default_sg = list(vcp.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )

    except Exception as e:
        print(e)

    # Wait for cluster to be available
    print("2.2 Wait for cluster to be available")
    while True:
        if redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] == 'available':
            break
        else:
            print("Cluster is not available yet. Sleeping for 10 seconds...")
            time.sleep(10)

    # Get cluster endpoint and port
    print("2.3 Get cluster endpoint and port")
    DWH_ENDPOINT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
    DWH_ROLE_ARN = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['IamRoles'][0]['IamRoleArn']
    DWH_CLUSTER_ARN = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterIdentifier']
    DWH_DB_NAME = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['DBName']
    DWH_DB_USER = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['MasterUsername']
    DWH_DB_PASSWORD = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['MasterUsername']
    DWH_PORT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Port']

if __name__ == '__main__':
    main()