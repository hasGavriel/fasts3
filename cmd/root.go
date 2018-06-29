package cmd

import (
	"fmt"
	"log"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "fasts3",
	Short: "A faster S3 utility",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if showVersion, err := cmd.Flags().GetBool("version"); err == nil && showVersion {
			versionCmd.Run(cmd, args)
			return
		}
	},
}

var (
	s3Client *s3.S3

	keyRegex    string
	delimiter   string
	searchDepth int
	maxParallel int
)

func init() {
	rootCmd.Flags().Bool("version", false, "Show the version")
	rootCmd.PersistentFlags().StringVar(&keyRegex, "key-regex", "", "Regex filter for keys")
	rootCmd.PersistentFlags().StringVar(&delimiter, "delimiter", "/", "Delimiter to use while listing")
	rootCmd.PersistentFlags().IntVar(&searchDepth, "search-depth", 0, "Dictates how many prefix groups to walk down")
	rootCmd.PersistentFlags().IntVarP(&maxParallel, "max-parallel", "p", 10, "Maximum number of calls to make to S3 simultaneously")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatal(err)
	}
	s3Client = s3.New(awsSession, aws.NewConfig())

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func validateS3URIs(pArgs ...cobra.PositionalArgs) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		for _, pArg := range pArgs {
			err := pArg(cmd, args)
			if err != nil {
				return err
			}
		}

		for _, a := range args {
			hasMatch, err := regexp.MatchString("^s3://", a)
			if err != nil {
				return err
			}
			if !hasMatch {
				return fmt.Errorf("%s not a valid S3 uri, Please enter a valid S3 uri. Ex: s3://mary/had/a/little/lamb", a)
			}
		}
		return nil
	}
}
