#!groovy
@Library('jenkins-pipeline-shared@develop') _

def projectName = 'registers-sml'
def distDir = 'build/dist/'
def server = Artifactory.server 'art-p-01'
def buildInfo = Artifactory.newBuildInfo()

pipeline {
    agent {node{label 'master'}}
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 15, unit: 'MINUTES')
        timestamps()
    }
    environment {
        SBT_HOME = tool name: 'sbt.13.13', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
        PATH = "${env.SBT_HOME}/bin:${env.PATH}"
        BUILD_ENV = "CI"
    }

    stages {
        stage('Init') {
            steps {
                sh 'git clean -fxd'
                script {
                    withEnv(){
                        colourText("info", "Building project ${projectName}")
                        colourText("info", "${getCurrentStage()}")
                    }
                }
            }
        }

    }
}