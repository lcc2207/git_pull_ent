import json
import decimal
import os
import git
import logging
import boto3
import time

from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

cb = boto3.client('codebuild')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('git_commits')
Key = boto3.dynamodb.conditions.Key

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def lsremote(url):
    remote_refs = {}
    g = git.cmd.Git()
    for ref in g.ls_remote(url).split('\n'):
        hash_ref_list = ref.split('\t')
        remote_refs[hash_ref_list[1]] = hash_ref_list[0]
    return remote_refs

def update_db(reponame, commit_id, action):
    logging.info(action)
    if action == 'add':
        response = table.put_item(
           Item={
                'git_repo_name': reponame,
                'commit_id': commit_id
                }
            )
    else:
        response = table.update_item(
           Key={'git_repo_name': reponame},
           UpdateExpression='SET commit_id = :commitid',
           ExpressionAttributeValues={':commitid': commit_id})

def check_db(reponame, commit_id):
    # query the database
    logging.info('reponame:' + reponame)
    response=table.query(KeyConditionExpression=Key('git_repo_name').eq(reponame))
    if response['Count'] == 0:
        status='add'
    else:
        if commit_id == response['Items'][0]['commit_id']:
            status='nothing'
        else:
            status='update'
    return(status)

def run_cb(reponame): #( event, context ):
  build = {'projectName': reponame}
  cbuild=cb.start_build( **build )
  buildId=(cbuild['build']['id'])
  buildSucceeded = False

  counter = 0
  while counter < 10:   #capped this, so it just fails if it takes too long
    time.sleep(5)
    counter = counter + 1
    theBuild = cb.batch_get_builds(ids=[buildId])
    buildStatus = theBuild['builds'][0]['buildStatus']
    logging.info(buildStatus)

    if buildStatus == 'SUCCEEDED':
      buildSucceeded = True
      break
    elif buildStatus == 'FAILED' or buildStatus == 'FAULT' or buildStatus == 'STOPPED' or buildStatus == 'TIMED_OUT':
      break

  return buildStatus

def lambda_function(reponame, repo_url):
    commit_id = lsremote(repo_url)['HEAD']
    status = check_db(reponame, commit_id)
    if status == 'nothing':
        logging.info('Nothing todo')
    else:
        run_cb(reponame)
        update_db(reponame, commit_id, status)
        logging.info('call codebuild')
        logging.info('run ' + status)

if __name__ == '__main__':
    lambda_function('testrepo1', 'git@github.com:lcc2207/testrepo1.git')
