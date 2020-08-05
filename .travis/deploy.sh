if [ ! -z "$TRAVIS_TAG" ]
then
    echo "on a tag -> $TRAVIS_TAG and therefore we will do nothing. Tagged releases are deployed to sonatype manually."
else
    echo "not on a tag -> deploying to sonatype..."
    FCREPO_VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
    
    echo "deploying to sonatype snapshot repo..."
    ./mvnw clean deploy --settings .travis/settings.xml -DskipTests=true -B -U
fi
