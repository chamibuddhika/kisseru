dot -Tpng -O cluster_app-before.dot
dot -Tpng -O cluster_app-after.dot

platform='unknown'
case "$OSTYPE" in
  darwin*)  platform='osx' ;; 
  linux*)   platform='linux' ;;
  cygwin*)  platform='cygwin' ;;
  *)        platform='linux' ;;
esac   

if [[ $platform == 'osx' ]]; then
   open cluster_app-before.dot.png
   open cluster_app-after.dot.png
elif [[ $platform == 'linux' ]]; then
   xdg-open cluster_app-before.dot.png
   xdg-open cluster_app-after.dot.png
elif [[ $platform == 'cygwin' ]]; then
   cygstart cluster_app-before.dot.png
   cygstart cluster_app-after.dot.png
fi
