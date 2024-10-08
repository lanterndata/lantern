if command -v uname &> /dev/null
then
  ARCH=$(uname -m)
elif command -v dpkg &> /dev/null
then
  ARCH=$(dpkg --print-architecture)
elif command -v apk &> /dev/null
then
  ARCH=$(apk --print-arch)
else
  echo "Could not detect system architecture. Please specify with ARCH env variable"
  exit 1
fi

# At this moment we only support mac and linux
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=mac;;
    *)          PLATFORM=${unameOut}
esac

export ARCH
export PLATFORM
