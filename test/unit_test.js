'use strict'

const {describe} = require('mocha')
const leadershipElection = require('./leadershipElection')
const logReplication = require('./logReplication')

describe('unit tests', () => {
  describe('leadership election', leadershipElection)
  describe('log replication', logReplication)
})
