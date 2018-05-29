#!groovy
@Library('jenkins-pipeline-shared@develop') _

def projectName = 'sbr-ci-poc'
def distDir = 'build/dist/'
def server = Artifactory.server 'art-p-01'
def buildInfo = Artifactory.newBuildInfo()

pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh 'mvn clean compile'
            }
        }
        stage('Unit Test') {
            steps {
                sh 'mvn test'
            }
        }
        stage('Package') {
            steps {
                sh 'mvn package'
            }
        }
    }
    post {
        always {
            junit '**/target/*-reports/*.xml'
            deleteDir()
        }
    }
}
