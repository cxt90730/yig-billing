package billing

import (
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/robfig/cron"
)

func Billing() {
	c := cron.New()
	if !helper.Conf.EnablePostBillingCron {
		postBilling()
	} else {
		c.AddFunc(helper.Conf.PostBillingSpec, postBilling)
	}
	collectMessage()
	c.Start()
}
