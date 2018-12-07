pipeline{
    agent { label 'slave_local' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "## Limpando o Workspace ##"'
                deleteDir()
            }
        }

        stage('SCM GitHub - Checkout'){
            steps{
                dir('projeto'){
		    
		    git branch: 'development',
                    checkout scm
			sh '''
		         
			 echo ${BRANCH_NAME} "origem"		
  		            case ${BRANCH_NAME} in
			        master)     	 FLOW="prd"       ;;
	   		        development)     FLOW="qas"       ;;
	    		       *)          	 FLOW="default"   ;;
		            esac
    			    echo ${FLOW} > flow.tmp
    			    cat flow.tmp
		           '''
                }
            }  
        }
        

        stage('Find directory to build'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Find directory to build ##"'
                    sh 'pwd'
                    sh 'tree'
                }
            }
        }
        

        stage('Build Dlkbigdata - dl-scala'){
            steps{
                dir('projeto/dl-scala'){
                    sh 'echo -e "## Build dl-scala ##"'
                    sh 'pwd'
                    sh 'sbt clean assembly'
                }
            }
        }

            stage('Clean S3'){
            steps{
                dir('projeto/dl-scala/target/scala-2.11'){
                    sh 'aws --version'
                    sh 'aws s3 ls'
                    sh 'pwd'
                    sh 'ls -lrt'
                    sh 'aws s3 rm s3://repo-lambda-teste/dl-scala-assembly-0.1.jar'
                }
            }
        }
        
        
            stage('Publisher S3'){
            steps{
                dir('projeto/dl-scala/target/scala-2.11'){
                    sh 'aws --version'
                    sh 'aws s3 ls'
                    sh 'pwd'
                    sh 'ls -lrt'
                    sh 'aws s3 cp *.jar s3://repo-lambda-teste/'
                }
            }
        }     
    }
}
