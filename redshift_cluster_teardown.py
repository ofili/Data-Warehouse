import boto3
import configparser


config = configparser.ConfigParser()
config.read('dwh.cfg')

# Load  DWH parameters from config file
KEY                         = config.get('AWS', 'KEY')
SECRET                      = config.get('AWS', 'SECRET')
DWH_CLUSTER_IDENTIFIER      = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_IAM_ROLE_NAME           = config.get('DWH', 'DWH_IAM_ROLE_NAME')



def delete_cluster(DWH_CLUSTER_IDENTIFIER):
    """
    This function will delete the Redshift cluster.
    """
    redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    # Delete the cluster
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

    # Wait for the cluster to be deleted
    print("Waiting for the cluster to be deleted...")
    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        except Exception as e:
            print(e)
            break
        if response['Clusters'][0]['ClusterStatus'] == 'deleting':
            print("Cluster is being deleted...")
            break
        else:
            print("Cluster is not being deleted...")
            break

def delete_role(DWH_IAM_ROLE_NAME):
    """
    This function will delete the Redshift cluster.
    """
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    # Wait for the role to be deleted
    print("Waiting for the role to be deleted...")
    while True:
        try:
            response = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
        except Exception as e:
            print(e)
            break
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("Role is being deleted...")
            break
        else:
            print("Role is not being deleted...")
            break
    


def main():
    """
    Main function.
    """

    # Delete the cluster
    delete_cluster(DWH_CLUSTER_IDENTIFIER)
    print("Cluster deleted")

    # Delete the role
    #delete_role(DWH_IAM_ROLE_NAME)
    print("Role deleted")

if __name__ == "__main__":
    main()
