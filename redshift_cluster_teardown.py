import boto3
import configparser

def main():
    """
    This function will teardown the Redshift cluster.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Get the Redshift client
    redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    # Get IAM client
    iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    # Get the cluster identifier
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    # Delete the cluster
    redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)

    # Delete the role
    iam.detach_role_policy(RoleName='RedshiftReadOnlyAccess', PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
    iam.delete_role(RoleName='RedshiftReadOnlyAccess')

    # Wait for the cluster to be deleted
    print("Waiting for the cluster to be deleted...")
    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
        except Exception as e:
            print(e)
            break
        if response['Clusters'][0]['ClusterStatus'] == 'deleting':
            print("Cluster is being deleted...")
            break
        else:
            print("Cluster is not being deleted...")
            break


if __name__ == "__main__":
    main()