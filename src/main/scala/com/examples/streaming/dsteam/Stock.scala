package com.examples.streaming.dsteam

import java.sql.Date

case class Stock(
                  company: String,
                  date: Date,
                  value: Double
                )
