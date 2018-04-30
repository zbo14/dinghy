'use strict'

const {
  expectCurrentTerm,
  expectOneLeader,
  initServers,
  startServers,
  stopServers,
  wait
} = require('./fixtures')

module.exports = () => {
  initServers(4000)
  startServers()
  expectCurrentTerm(-1)
  wait(300)
  expectOneLeader()
  expectCurrentTerm(0)
  stopServers()
  startServers()
  wait(300)
  expectOneLeader()
  expectCurrentTerm(1)
  stopServers()
}
